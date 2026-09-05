using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Manages the primary index (.PX) B-tree for a Paradox table.
    /// Keeps the .PX file in sync whenever records are inserted,
    /// updated, or deleted in the .DB file.
    ///
    /// IMPORTANT: Empirically decoded via SQLRunner byte-level comparison
    /// (see project history), Paradox leaf entries are per-DB-BLOCK, not
    /// per-row. Each leaf entry stores the key of the FIRST row currently
    /// in a given .DB data block, plus a 6-byte pointer:
    /// blockNumber(ushort) + recordCount(ushort) + reserved(ushort), each
    /// sign-flip encoded (value ^ 0x8000) and written big-endian (same
    /// encoding KeySerializer uses for Short fields). The block header is
    /// 6 bytes: leftChildBlockNumber(ushort) + reserved(ushort) +
    /// usedBytes(ushort), where usedBytes = (entryCount - 1) * entrySize
    /// (confirmed for entryCount 1, 2, and 3).
    /// </summary>
    internal class PrimaryIndexFile : IDisposable
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        private const int POINTER_SIZE = 6; // blockNumber(2) + recordCount(2) + reserved(2)
        private const int HEADER_SIZE  = 6; // leftChild(2) + reserved(2) + usedBytes(2)

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly ParadoxFile pxFile;
        private readonly ParadoxFile.FieldInfo[] primaryKeyFields;
        private readonly int         keyDataSize;
        private readonly int         entrySize;
        private readonly int         blockCapacity;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public PrimaryIndexFile(string pxFilePath, ParadoxFile.FieldInfo[] primaryKeyFields)
        {
            this.primaryKeyFields = primaryKeyFields;

            foreach (var f in primaryKeyFields)
                keyDataSize += f.fSize;

            entrySize     = keyDataSize + POINTER_SIZE;
            pxFile        = new ParadoxFile(pxFilePath);
            blockCapacity = pxFile.maxTableSize * 0x400 - HEADER_SIZE;

            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile] Opened '{pxFilePath}': " +
                $"rootBlock={pxFile.pxRootBlockId}, headerSize={pxFile.headerSize}, " +
                $"maxTableSize={pxFile.maxTableSize}, blockSize={pxFile.maxTableSize * 0x400}, " +
                $"streamLength={pxFile.stream.Length}, keyDataSize={keyDataSize}, " +
                $"entrySize={entrySize}, blockCapacity={blockCapacity}");
        }

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        /// <summary>
        /// Called whenever a .DB data block's contents change (a record was
        /// inserted, updated, or deleted in that block). This is the ONLY
        /// entry point index maintenance needs, because Paradox leaf entries
        /// track the whole block, not individual rows: given the current key
        /// of the block's first row and its current record count, the
        /// correct leaf entry can always be derived.
        /// </summary>
        /// <param name="firstRowKeyValues">
        /// Primary key field values of the record at index 0 in the block,
        /// or null if the block is now empty (recordCount == 0).
        /// </param>
        /// <param name="dbBlockNumber">The affected .DB block number.</param>
        /// <param name="recordCount">The block's current record count.</param>
        public void OnBlockChanged(object[] firstRowKeyValues, ushort dbBlockNumber, int recordCount)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile.OnBlockChanged] dbBlockNumber={dbBlockNumber}, recordCount={recordCount}");

            if (recordCount <= 0)
            {
                RemoveBlockEntry(dbBlockNumber);
                return;
            }

            byte[] keyData = KeySerializer.Serialize(firstRowKeyValues, primaryKeyFields);

            if (pxFile.stream.Length <= pxFile.headerSize)
            {
                var newLeaf = AllocateBlock();
                newLeaf.Entries.Add(new PxEntry(keyData, dbBlockNumber, (ushort)recordCount));
                WriteBlock(newLeaf);
                UpdateRootBlockId(newLeaf.BlockNumber);
                UpdateLevelCount(1);
                UpdateRecordCount(pxFile.RecordCount + 1);
                return;
            }

            var existing = FindEntryForBlock(dbBlockNumber);
            if (existing.Block != null)
            {
                var entry = existing.Block.Entries[existing.Index];
                entry.KeyData     = keyData;
                entry.RecordCount = (ushort)recordCount;
                WriteBlock(existing.Block);
                return;
            }

            BTreeInsert(new PxEntry(keyData, dbBlockNumber, (ushort)recordCount));
            UpdateRecordCount(pxFile.RecordCount + 1);
        }

        /// <summary>
        /// Previously mirrored the parent .DB file's changeCount1/changeCount2 bytes
        /// (offset 0x2D) into this .PX file. Disproven by direct experiment: a known-good
        /// SQLRunner-generated ground-truth table has changeCount = 00,00 in its .PX while
        /// the .DB has a nonzero value, and Pdxrbld reports it as "no errors found". Forcing
        /// the .PX changeCount to match the .DB's did NOT resolve "Index version does not
        /// match table version" in a standalone repro. So this is a no-op again.
        /// </summary>
        public void SyncChangeCount(byte changeCount1, byte changeCount2)
        {
            // Intentionally no-op; see summary above.
        }

        /// <summary>
        /// Mirrors the parent .DB file's autoIncVal (offset 0x49) into this
        /// .PX file. BDE/Pdxrbld considers the index out of date if this
        /// doesn't match the table's autoIncVal after an AutoInc field is
        /// assigned.
        /// </summary>
        public void SyncAutoIncVal(int autoIncVal)
        {
            pxFile.autoIncVal = autoIncVal;
            pxFile.stream.Position = 0x49;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(pxFile.autoIncVal);
        }

        /// <summary>
        /// Mirrors the parent .DB file's V4Hdr changeCount4 (offset 0x70) into
        /// this .PX file. BDE/Pdxrbld compares this "table version" counter
        /// against the index's own copy to decide whether the index is out
        /// of date.
        /// </summary>
        public void SyncTableVersion(short changeCount4)
        {
            pxFile.stream.Position = 0x70;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(changeCount4);
        }

        /// <summary>
        /// Increments the single-byte write counter at offset 0x2C
        /// (pxlib's unknown2Bx2C[1]) once per index write. Discovered via
        /// multi-pass SQLRunner testing: a valid .PX file increments this
        /// byte by exactly 1 on every INSERT, independently of the parent
        /// .DB file's changeCount1/changeCount2 (offset 0x2D), which the
        /// .PX file does NOT mirror (it stays 0x00). This is being tried as
        /// the actual "index version" field BDE/Pdxrbld checks.
        /// </summary>
        public void IncrementWriteCounter()
        {
            pxFile.stream.Position = 0x2C;
            int current = pxFile.stream.ReadByte();
            if (current < 0) current = 0;
            byte next = (byte)(current + 1);
            pxFile.stream.Position = 0x2C;
            pxFile.stream.WriteByte(next);
        }

        // ----------------------------------------------------------------
        // Block-number lookup (leaf entries only represent .DB blocks)
        // ----------------------------------------------------------------

        private class FoundEntry
        {
            public PxBlock Block;
            public int Index;
        }

        private FoundEntry FindEntryForBlock(ushort dbBlockNumber)
        {
            if (pxFile.stream.Length <= pxFile.headerSize) return new FoundEntry { Block = null, Index = -1 };
            return FindEntryForBlockRecursive(ReadBlock(pxFile.pxRootBlockId), dbBlockNumber);
        }

        private FoundEntry FindEntryForBlockRecursive(PxBlock node, ushort dbBlockNumber)
        {
            if (node.IsLeaf)
            {
                for (int i = 0; i < node.Entries.Count; i++)
                    if (node.Entries[i].BlockNumber == dbBlockNumber)
                        return new FoundEntry { Block = node, Index = i };
                return new FoundEntry { Block = null, Index = -1 };
            }

            if (node.LeftChildBlockNumber != 0)
            {
                var r = FindEntryForBlockRecursive(ReadBlock(node.LeftChildBlockNumber), dbBlockNumber);
                if (r.Block != null) return r;
            }
            foreach (var e in node.Entries)
            {
                var r = FindEntryForBlockRecursive(ReadBlock(e.BlockNumber), dbBlockNumber);
                if (r.Block != null) return r;
            }
            return new FoundEntry { Block = null, Index = -1 };
        }

        private void RemoveBlockEntry(ushort dbBlockNumber)
        {
            var found = FindEntryForBlock(dbBlockNumber);
            if (found.Block == null) return;
            byte[] key = found.Block.Entries[found.Index].KeyData;
            BTreeDelete(key);
        }

        // ----------------------------------------------------------------
        // B-Tree: Insert (used only when a brand-new .DB block needs its
        // own leaf entry; existing-block record-count/key updates are
        // handled directly in OnBlockChanged without tree restructuring)
        // ----------------------------------------------------------------

        private void BTreeInsert(PxEntry entry)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile.BTreeInsert] rootBlock={pxFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(entry.KeyData)}");

            var root = ReadBlock(pxFile.pxRootBlockId);
            if (root.IsFull(entrySize))
            {
                var newRoot = AllocateBlock();
                newRoot.LeftChildBlockNumber = pxFile.pxRootBlockId;
                SplitChild(newRoot, 0, root);
                InsertNonFull(newRoot, entry);
                WriteBlock(newRoot);
                UpdateRootBlockId(newRoot.BlockNumber);
                UpdateLevelCount((byte)(pxFile.pxLevelCount + 1));
            }
            else
            {
                InsertNonFull(root, entry);
            }
        }

        private void InsertNonFull(PxBlock node, PxEntry entry)
        {
            int i = node.Entries.Count - 1;
            if (node.IsLeaf)
            {
                node.Entries.Add(null);
                while (i >= 0 && CompareKeys(entry.KeyData, node.Entries[i].KeyData) < 0)
                {
                    node.Entries[i + 1] = node.Entries[i];
                    i--;
                }
                node.Entries[i + 1] = entry;
                WriteBlock(node);
            }
            else
            {
                while (i >= 0 && CompareKeys(entry.KeyData, node.Entries[i].KeyData) < 0)
                    i--;
                i++;
                var child = ReadBlock(i < node.Entries.Count ? node.Entries[i].BlockNumber : node.LeftChildBlockNumber);
                if (child.IsFull(entrySize))
                {
                    SplitChild(node, i, child);
                    if (CompareKeys(entry.KeyData, node.Entries[i].KeyData) > 0) i++;
                }
                var target = i < node.Entries.Count ? node.Entries[i].BlockNumber : node.LeftChildBlockNumber;
                InsertNonFull(ReadBlock(target), entry);
            }
        }

        private void SplitChild(PxBlock parent, int childIndex, PxBlock fullChild)
        {
            var newChild = AllocateBlock();
            newChild.LeftChildBlockNumber = fullChild.LeftChildBlockNumber;
            int median       = fullChild.Entries.Count / 2;
            var medianEntry  = fullChild.Entries[median];
            newChild.Entries = fullChild.Entries.GetRange(
                                   median + 1,
                                   fullChild.Entries.Count - median - 1);
            fullChild.Entries = fullChild.Entries.GetRange(0, median);
            parent.Entries.Insert(childIndex,
                new PxEntry(medianEntry.KeyData, newChild.BlockNumber, 0));
            WriteBlock(fullChild);
            WriteBlock(newChild);
            WriteBlock(parent);
        }

        // ----------------------------------------------------------------
        // B-Tree: Delete (used only when a .DB block's last record is
        // removed, so its leaf entry must be removed entirely)
        // ----------------------------------------------------------------

        private void BTreeDelete(byte[] keyData)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile.BTreeDelete] rootBlock={pxFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(keyData)}");

            if (pxFile.stream.Length <= pxFile.headerSize)
                return; // Nothing to delete from an empty tree.

            var root = ReadBlock(pxFile.pxRootBlockId);
            DeleteFromNode(root, keyData);
            if (root.Entries.Count == 0 && !root.IsLeaf)
            {
                UpdateRootBlockId(root.LeftChildBlockNumber);
                FreeBlock(root.BlockNumber);
                if (pxFile.pxLevelCount > 0)
                    UpdateLevelCount((byte)(pxFile.pxLevelCount - 1));
            }
            UpdateRecordCount(pxFile.RecordCount - 1);
        }

        private void DeleteFromNode(PxBlock node, byte[] keyData)
        {
            int i = FindKeyIndex(node, keyData);
            if (i < node.Entries.Count &&
                CompareKeys(keyData, node.Entries[i].KeyData) == 0)
            {
                if (node.IsLeaf)
                {
                    node.Entries.RemoveAt(i);
                    WriteBlock(node);
                }
                else
                {
                    var pred = GetPredecessor(node, i);
                    node.Entries[i] = pred;
                    WriteBlock(node);
                    DeleteFromNode(ReadBlock(node.Entries[i].BlockNumber), pred.KeyData);
                }
            }
            else if (!node.IsLeaf)
            {
                var child = ReadBlock(i < node.Entries.Count
                    ? node.Entries[i].BlockNumber
                    : node.LeftChildBlockNumber);
                DeleteFromNode(child, keyData);
            }
        }

        private PxEntry GetPredecessor(PxBlock node, int idx)
        {
            var cur = ReadBlock(node.Entries[idx].BlockNumber);
            while (!cur.IsLeaf)
                cur = ReadBlock(cur.Entries[cur.Entries.Count - 1].BlockNumber);
            return cur.Entries[cur.Entries.Count - 1];
        }

        // ----------------------------------------------------------------
        // Block I/O
        // ----------------------------------------------------------------

        private PxBlock ReadBlock(ushort blockNumber)
        {
            var block = new PxBlock
            {
                BlockNumber = blockNumber,
                Capacity    = blockCapacity
            };
            int  blockSize = pxFile.maxTableSize * 0x400;
            long pos       = pxFile.headerSize + (long)(blockNumber - 1) * blockSize;

            if (pos < pxFile.headerSize || pos + blockSize > pxFile.stream.Length)
                throw new InvalidOperationException(
                    $"[PrimaryIndexFile.ReadBlock] Block {blockNumber} is out of range. " +
                    $"offset={pos}, blockSize={blockSize}, " +
                    $"streamLength={pxFile.stream.Length}, headerSize={pxFile.headerSize}.");

            pxFile.stream.Position = pos;
            using (var r = new BinaryReader(pxFile.stream, Encoding.Default, leaveOpen: true))
            {
                block.LeftChildBlockNumber = r.ReadUInt16();
                r.ReadUInt16(); // reserved
                r.ReadUInt16(); // usedBytes (derived from Entries at write time; not needed on read)
                int bytesRead = HEADER_SIZE;
                while (bytesRead + entrySize <= blockCapacity)
                {
                    byte[] key = r.ReadBytes(keyDataSize);
                    if (IsEmptyKey(key)) break;
                    ushort bn = ReadSignFlippedUInt16(r);
                    ushort rc = ReadSignFlippedUInt16(r);
                    r.ReadBytes(2); // reserved word
                    block.Entries.Add(new PxEntry(key, bn, rc));
                    bytesRead += entrySize;
                }
            }
            return block;
        }

        private void WriteBlock(PxBlock block)
        {
            int  blockSize = pxFile.maxTableSize * 0x400;
            long pos       = pxFile.headerSize + (long)(block.BlockNumber - 1) * blockSize;
            pxFile.stream.Position = pos;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
            {
                ushort usedBytes = block.Entries.Count <= 1
                    ? (ushort)0
                    : (ushort)((block.Entries.Count - 1) * entrySize);

                w.Write(block.LeftChildBlockNumber);
                w.Write((ushort)0); // reserved
                w.Write(usedBytes);
                foreach (var e in block.Entries)
                {
                    w.Write(e.KeyData);
                    WriteSignFlippedUInt16(w, e.BlockNumber);
                    WriteSignFlippedUInt16(w, e.RecordCount);
                    WriteSignFlippedUInt16(w, 0); // reserved word, sign-flipped 0 => 0x8000
                }
                int used = HEADER_SIZE + block.UsedSize;
                int rem  = blockSize - used;
                if (rem > 0) w.Write(new byte[rem]);
            }
        }

        private PxBlock AllocateBlock()
        {
            int    blockSize = pxFile.maxTableSize * 0x400;
            // Block numbers are 1-based: existing block count gives us the next 1-based ID.
            ushort n         = (ushort)((pxFile.stream.Length - pxFile.headerSize) / blockSize + 1);
            pxFile.stream.SetLength(pxFile.stream.Length + blockSize);
            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile.AllocateBlock] Allocated block {n} at " +
                $"offset={pxFile.headerSize + (long)(n - 1) * blockSize}, newStreamLength={pxFile.stream.Length}");

            // Keep the .PX file's own block-chain bookkeeping fields (nextBlock,
            // fileBlocks, firstBlock, lastBlock @ 0xA-0x11) in sync with the
            // actual number of allocated blocks. BDE/Pdxrbld considers the index
            // out of date/corrupt if these aren't updated.
            if (n == 1) pxFile.firstBlock = n;
            pxFile.nextBlock  = n;
            pxFile.lastBlock  = n;
            pxFile.fileBlocks = n;
            pxFile.stream.Position = 0xA;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
            {
                w.Write(pxFile.nextBlock);
                w.Write(pxFile.fileBlocks);
                w.Write(pxFile.firstBlock);
                w.Write(pxFile.lastBlock);
            }

            // maxBlocks (word @ 0x3A) tracks allocated capacity; BDE/Pdxrbld flags
            // the index as corrupt if the actual block count ever exceeds it.
            if (n > pxFile.maxBlocks)
            {
                pxFile.maxBlocks = n;
                pxFile.stream.Position = 0x3A;
                using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                    w.Write(pxFile.maxBlocks);
            }

            return new PxBlock { BlockNumber = n, Capacity = blockCapacity };
        }

        private void FreeBlock(ushort blockNumber)
        {
            int  blockSize = pxFile.maxTableSize * 0x400;
            long pos       = pxFile.headerSize + (long)(blockNumber - 1) * blockSize;
            System.Diagnostics.Debug.WriteLine(
                $"[PrimaryIndexFile.FreeBlock] Zeroing block {blockNumber} at offset={pos}");
            pxFile.stream.Position = pos;
            pxFile.stream.Write(new byte[blockSize], 0, blockSize);
        }

        private void UpdateRootBlockId(ushort newRootId)
        {
            pxFile.pxRootBlockId = newRootId;
            pxFile.stream.Position = 0x1E;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(newRootId);
        }

        /// <summary>
        /// Keeps numIndexLevels (@ 0x20) in sync with the actual B-tree depth.
        /// BDE/Pdxrbld considers the index out of date/corrupt if this isn't
        /// updated when the tree grows or shrinks.
        /// </summary>
        private void UpdateLevelCount(byte levelCount)
        {
            pxFile.pxLevelCount = levelCount;
            pxFile.stream.Position = 0x20;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(pxFile.pxLevelCount);
        }

        /// <summary>
        /// Keeps the .PX file's own RecordCount header field (@ 0x06, int32) in
        /// sync with the number of leaf entries (one per .DB block) stored in
        /// the index. BDE/Pdxrbld considers the index out of date/corrupt if
        /// this doesn't match the actual number of leaf entries.
        /// </summary>
        private void UpdateRecordCount(int recordCount)
        {
            pxFile.RecordCount = recordCount;
            pxFile.stream.Position = 0x6;
            using (var w = new BinaryWriter(pxFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(pxFile.RecordCount);
        }

        // ----------------------------------------------------------------
        // Key helpers
        // ----------------------------------------------------------------

        private int CompareKeys(byte[] a, byte[] b)
            => KeySerializer.Compare(a, b, primaryKeyFields);

        private int FindKeyIndex(PxBlock node, byte[] keyData)
        {
            int i = 0;
            while (i < node.Entries.Count &&
                   CompareKeys(keyData, node.Entries[i].KeyData) > 0) i++;
            return i;
        }

        private bool IsEmptyKey(byte[] key)
        {
            foreach (var b in key) if (b != 0) return false;
            return true;
        }

        private static ushort ReadSignFlippedUInt16(BinaryReader r)
        {
            byte hi = r.ReadByte();
            byte lo = r.ReadByte();
            ushort encoded = (ushort)((hi << 8) | lo);
            return PxEntry.FlipWord(encoded);
        }

        private static void WriteSignFlippedUInt16(BinaryWriter w, ushort value)
        {
            ushort encoded = PxEntry.FlipWord(value);
            w.Write((byte)(encoded >> 8));
            w.Write((byte)(encoded & 0xFF));
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose() => pxFile?.Dispose();
    }
}
