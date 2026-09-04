using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Manages a single secondary index file (.Xnn or .Xgn) for a Paradox table.
    /// Uses the same B-tree algorithm as PrimaryIndex but indexes
    /// non-primary-key fields, identified by their position in the parent table.
    ///
    /// IMPORTANT: Empirically decoded via SQLRunner byte-level comparison
    /// (see project history), Paradox leaf entries are per-DB-BLOCK, not
    /// per-row. Each leaf entry stores the key of the FIRST row currently
    /// in a given .DB data block, plus a 6-byte pointer:
    /// blockNumber(ushort) + recordCount(ushort) + reserved(ushort), each
    /// sign-flip encoded (value ^ 0x8000) and written big-endian (same
    /// encoding KeySerializer uses for Short fields). The block header is
    /// 6 bytes: leftChildBlockNumber(ushort) + reserved(ushort) +
    /// usedBytes(ushort), where usedBytes = (entryCount - 1) * entrySize.
    /// </summary>
    internal class SecondaryIndexFile : IDisposable
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        private const int POINTER_SIZE = 6; // blockNumber(2) + recordCount(2) + reserved(2)
        private const int HEADER_SIZE  = 6; // leftChild(2) + reserved(2) + usedBytes(2)

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly ParadoxFile indexFile;
        private readonly ParadoxFile.FieldInfo[] indexedFields;
        private readonly int[]       fieldIndices;  // 0-based positions in parent table
        private readonly int         keyDataSize;
        private readonly int         entrySize;
        private readonly int         blockCapacity;
        // XgnFile types use 0-based block numbers; all others (YgnFile, XnnFile) use 1-based.
        private readonly ushort      blockBase;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public SecondaryIndexFile(string indexFilePath, ParadoxFile.FieldInfo[] indexedFields, int[] fieldIndices)
        {
            this.indexedFields = indexedFields;
            this.fieldIndices  = fieldIndices;

            foreach (var f in indexedFields)
                keyDataSize += f.fSize;

            entrySize     = keyDataSize + POINTER_SIZE;
            indexFile     = new ParadoxFile(indexFilePath);
            blockCapacity = indexFile.maxTableSize * 0x400 - HEADER_SIZE;
            blockBase     = (indexFile.FileType == ParadoxFileType.XgnFileNonInc ||
                             indexFile.FileType == ParadoxFileType.XgnFileInc) ? (ushort)0 : (ushort)1;

            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile] Opened '{indexFilePath}': " +
                $"rootBlock={indexFile.pxRootBlockId}, headerSize={indexFile.headerSize}, " +
                $"maxTableSize={indexFile.maxTableSize}, blockSize={indexFile.maxTableSize * 0x400}, " +
                $"streamLength={indexFile.stream.Length}, keyDataSize={keyDataSize}, " +
                $"entrySize={entrySize}, blockCapacity={blockCapacity}, " +
                $"blockBase={blockBase}, indexFieldNumber={indexFile.indexFieldNumber}, " +
                $"fieldIndices=[{string.Join(",", fieldIndices)}]");
        }

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        /// <summary>
        /// Called whenever a .DB data block's contents change (a record was
        /// inserted, updated, or deleted in that block). This is the ONLY
        /// entry point index maintenance needs, because Paradox leaf entries
        /// track the whole block, not individual rows: given the current key
        /// of the block's first row (using this index's own fields) and its
        /// current record count, the correct leaf entry can always be derived.
        /// </summary>
        /// <param name="firstRowAllFieldValues">
        /// All field values of the record at index 0 in the block, or null
        /// if the block is now empty (recordCount == 0).
        /// </param>
        /// <param name="blockNumber">The affected .DB block number.</param>
        /// <param name="recordCount">The block's current record count.</param>
        public void OnBlockChanged(object[] firstRowAllFieldValues, ushort blockNumber, int recordCount)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.OnBlockChanged] blockNumber={blockNumber}, recordCount={recordCount}");

            if (recordCount <= 0)
            {
                RemoveBlockEntry(blockNumber);
                return;
            }

            byte[] keyData = KeySerializer.Serialize(ExtractIndexValues(firstRowAllFieldValues), indexedFields);

            if (indexFile.stream.Length <= indexFile.headerSize)
            {
                var newLeaf = AllocateBlock();
                newLeaf.Entries.Add(new PxEntry(keyData, blockNumber, (ushort)recordCount));
                WriteBlock(newLeaf);
                UpdateRootBlockId(newLeaf.BlockNumber);
                UpdateLevelCount(1);
                UpdateRecordCount(indexFile.RecordCount + 1);
                return;
            }

            var existing = FindEntryForBlock(blockNumber);
            if (existing.Block != null)
            {
                var entry = existing.Block.Entries[existing.Index];
                entry.KeyData     = keyData;
                entry.RecordCount = (ushort)recordCount;
                WriteBlock(existing.Block);
                return;
            }

            BTreeInsert(new PxEntry(keyData, blockNumber, (ushort)recordCount));
            UpdateRecordCount(indexFile.RecordCount + 1);
        }

        /// <summary>
        /// Previously mirrored the parent .DB file's changeCount1/changeCount2 bytes
        /// (offset 0x2D) into this index file. See <see cref="PrimaryIndex.SyncChangeCount"/>
        /// for why this is a no-op; disproven by direct experiment.
        /// </summary>
        public void SyncChangeCount(byte changeCount1, byte changeCount2)
        {
            // Intentionally no-op; see summary above.
        }

        /// <summary>
        /// Mirrors the parent .DB file's autoIncVal (offset 0x49) into this
        /// index file. BDE/Pdxrbld considers the index out of date if this
        /// doesn't match the table's autoIncVal after an AutoInc field is
        /// assigned.
        /// </summary>
        public void SyncAutoIncVal(int autoIncVal)
        {
            indexFile.autoIncVal = autoIncVal;
            indexFile.stream.Position = 0x49;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(indexFile.autoIncVal);
        }

        /// <summary>
        /// Mirrors the parent .DB file's V4Hdr changeCount4 (offset 0x70) into
        /// this index file. BDE/Pdxrbld compares this "table version" counter
        /// against the index's own copy to decide whether the index is out
        /// of date.
        /// </summary>
        public void SyncTableVersion(short changeCount4)
        {
            indexFile.stream.Position = 0x70;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(changeCount4);
        }

        /// <summary>
        /// Increments the single-byte write counter at offset 0x2C
        /// (pxlib's unknown2Bx2C[1]) once per index write. See
        /// <see cref="PrimaryIndex.IncrementWriteCounter"/> for rationale.
        /// </summary>
        public void IncrementWriteCounter()
        {
            indexFile.stream.Position = 0x2C;
            int current = indexFile.stream.ReadByte();
            if (current < 0) current = 0;
            byte next = (byte)(current + 1);
            indexFile.stream.Position = 0x2C;
            indexFile.stream.WriteByte(next);
        }

        // ----------------------------------------------------------------
        // Field extraction
        // ----------------------------------------------------------------

        private object[] ExtractIndexValues(object[] allValues)
        {
            var result = new object[fieldIndices.Length];
            for (int i = 0; i < fieldIndices.Length; i++)
            {
                int fi = fieldIndices[i];
                result[i] = (allValues != null && fi < allValues.Length)
                    ? allValues[fi] : null;
            }
            return result;
        }

        // ----------------------------------------------------------------
        // Block-number lookup (leaf entries only represent .DB blocks)
        // ----------------------------------------------------------------

        private class FoundEntry
        {
            public PxBlock Block;
            public int Index;
        }

        private FoundEntry FindEntryForBlock(ushort blockNumber)
        {
            if (indexFile.stream.Length <= indexFile.headerSize) return new FoundEntry { Block = null, Index = -1 };
            return FindEntryForBlockRecursive(ReadBlock(indexFile.pxRootBlockId), blockNumber);
        }

        private FoundEntry FindEntryForBlockRecursive(PxBlock node, ushort blockNumber)
        {
            if (node.IsLeaf)
            {
                for (int i = 0; i < node.Entries.Count; i++)
                    if (node.Entries[i].BlockNumber == blockNumber)
                        return new FoundEntry { Block = node, Index = i };
                return new FoundEntry { Block = null, Index = -1 };
            }

            if (node.LeftChildBlockNumber != 0)
            {
                var r = FindEntryForBlockRecursive(ReadBlock(node.LeftChildBlockNumber), blockNumber);
                if (r.Block != null) return r;
            }
            foreach (var e in node.Entries)
            {
                var r = FindEntryForBlockRecursive(ReadBlock(e.BlockNumber), blockNumber);
                if (r.Block != null) return r;
            }
            return new FoundEntry { Block = null, Index = -1 };
        }

        private void RemoveBlockEntry(ushort blockNumber)
        {
            var found = FindEntryForBlock(blockNumber);
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
                $"[SecondaryIndexFile.BTreeInsert] rootBlock={indexFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(entry.KeyData)}");

            var root = ReadBlock(indexFile.pxRootBlockId);
            if (root.IsFull(entrySize))
            {
                var newRoot = AllocateBlock();
                newRoot.LeftChildBlockNumber = indexFile.pxRootBlockId;
                SplitChild(newRoot, 0, root);
                InsertNonFull(newRoot, entry);
                WriteBlock(newRoot);
                UpdateRootBlockId(newRoot.BlockNumber);
                UpdateLevelCount((byte)(indexFile.pxLevelCount + 1));
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
            int median        = fullChild.Entries.Count / 2;
            var medianEntry   = fullChild.Entries[median];
            newChild.Entries  = fullChild.Entries.GetRange(
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
                $"[SecondaryIndexFile.BTreeDelete] rootBlock={indexFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(keyData)}");

            if (indexFile.stream.Length <= indexFile.headerSize)
                return; // Nothing to delete from an empty tree.

            var root = ReadBlock(indexFile.pxRootBlockId);
            DeleteFromNode(root, keyData);
            if (root.Entries.Count == 0 && !root.IsLeaf)
            {
                UpdateRootBlockId(root.LeftChildBlockNumber);
                FreeBlock(root.BlockNumber);
                if (indexFile.pxLevelCount > 0)
                    UpdateLevelCount((byte)(indexFile.pxLevelCount - 1));
            }
            UpdateRecordCount(indexFile.RecordCount - 1);
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
            var block = new PxBlock { BlockNumber = blockNumber, Capacity = blockCapacity };
            long pos  = indexFile.headerSize + (long)(blockNumber - blockBase) * indexFile.maxTableSize * 0x400;
            indexFile.stream.Position = pos;
            using (var r = new BinaryReader(indexFile.stream, Encoding.Default, leaveOpen: true))
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
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos       = indexFile.headerSize + (long)(block.BlockNumber - blockBase) * blockSize;
            indexFile.stream.Position = pos;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
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
                int rem = blockSize - used;
                if (rem > 0) w.Write(new byte[rem]);
            }
        }

        private PxBlock AllocateBlock()
        {
            int    blockSize = indexFile.maxTableSize * 0x400;
            // New block number follows the file's block base convention (0-based for XgnFile, 1-based otherwise).
            ushort n         = (ushort)((indexFile.stream.Length - indexFile.headerSize) / blockSize + blockBase);
            indexFile.stream.SetLength(indexFile.stream.Length + blockSize);
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.AllocateBlock] Allocated block {n} at " +
                $"offset={indexFile.headerSize + (long)(n - blockBase) * blockSize}, newStreamLength={indexFile.stream.Length}");

            // Keep the index file's own block-chain bookkeeping fields (nextBlock,
            // fileBlocks, firstBlock, lastBlock @ 0xA-0x11) in sync with the
            // actual number of allocated blocks. BDE/Pdxrbld considers the index
            // out of date/corrupt if these aren't updated.
            ushort blockCountFromBase = (ushort)(n - blockBase + 1);
            if (blockCountFromBase == 1) indexFile.firstBlock = n;
            indexFile.nextBlock  = n;
            indexFile.lastBlock  = n;
            indexFile.fileBlocks = blockCountFromBase;
            indexFile.stream.Position = 0xA;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
            {
                w.Write(indexFile.nextBlock);
                w.Write(indexFile.fileBlocks);
                w.Write(indexFile.firstBlock);
                w.Write(indexFile.lastBlock);
            }

            // maxBlocks (word @ 0x3A) tracks allocated capacity; BDE/Pdxrbld flags
            // the index as corrupt if the actual block count ever exceeds it.
            if (blockCountFromBase > indexFile.maxBlocks)
            {
                indexFile.maxBlocks = blockCountFromBase;
                indexFile.stream.Position = 0x3A;
                using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                    w.Write(indexFile.maxBlocks);
            }

            return new PxBlock { BlockNumber = n, Capacity = blockCapacity };
        }

        private void FreeBlock(ushort blockNumber)
        {
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos       = indexFile.headerSize + (long)(blockNumber - blockBase) * blockSize;
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.FreeBlock] Zeroing block {blockNumber} at offset={pos}");
            indexFile.stream.Position = pos;
            indexFile.stream.Write(new byte[blockSize], 0, blockSize);
        }

        private void UpdateRootBlockId(ushort newRootId)
        {
            indexFile.stream.Position = 0x1E;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(newRootId);
        }

        /// <summary>
        /// Keeps numIndexLevels (@ 0x20) in sync with the actual B-tree depth.
        /// BDE/Pdxrbld considers the index out of date/corrupt if this isn't
        /// updated when the tree grows or shrinks.
        /// </summary>
        private void UpdateLevelCount(byte levelCount)
        {
            indexFile.pxLevelCount = levelCount;
            indexFile.stream.Position = 0x20;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(indexFile.pxLevelCount);
        }

        /// <summary>
        /// Keeps this index file's own RecordCount header field (@ 0x06, int32)
        /// in sync with the number of leaf entries (one per .DB block) stored
        /// in the index. BDE/Pdxrbld considers the index out of date/corrupt
        /// if this doesn't match the actual number of leaf entries.
        /// </summary>
        private void UpdateRecordCount(int recordCount)
        {
            indexFile.RecordCount = recordCount;
            indexFile.stream.Position = 0x6;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(indexFile.RecordCount);
        }

        // ----------------------------------------------------------------
        // Key helpers
        // ----------------------------------------------------------------

        private int CompareKeys(byte[] a, byte[] b)
            => KeySerializer.Compare(a, b, indexedFields);

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

        public void Dispose() => indexFile?.Dispose();
    }
}
