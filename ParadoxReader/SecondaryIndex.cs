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
    /// </summary>
    internal class SecondaryIndex : IDisposable
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        private const int POINTER_SIZE        = 4;
        private const int LEFT_CHILD_PTR_SIZE = 4;

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

        public SecondaryIndex(string indexFilePath, ParadoxFile.FieldInfo[] indexedFields, int[] fieldIndices)
        {
            this.indexedFields = indexedFields;
            this.fieldIndices  = fieldIndices;

            foreach (var f in indexedFields)
                keyDataSize += f.fSize;

            entrySize     = keyDataSize + POINTER_SIZE;
            indexFile     = new ParadoxFile(indexFilePath);
            blockCapacity = indexFile.maxTableSize * 0x400 - LEFT_CHILD_PTR_SIZE;
            blockBase     = (indexFile.FileType == ParadoxFileType.XgnFileNonInc ||
                             indexFile.FileType == ParadoxFileType.XgnFileInc) ? (ushort)0 : (ushort)1;

            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndex] Opened '{indexFilePath}': " +
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

        public void OnInsert(object[] allFieldValues, ushort blockNumber, ushort recordIndex)
        {
            byte[] keyData = KeySerializer.Serialize(
                ExtractIndexValues(allFieldValues), indexedFields);
            BTreeInsert(new PxEntry(keyData, blockNumber, recordIndex));
        }

        public void OnUpdate(object[] oldAll, object[] newAll,
                             ushort blockNumber, ushort recordIndex)
        {
            byte[] oldKey = KeySerializer.Serialize(ExtractIndexValues(oldAll), indexedFields);
            byte[] newKey = KeySerializer.Serialize(ExtractIndexValues(newAll), indexedFields);
            if (KeySerializer.Compare(oldKey, newKey, indexedFields) == 0) return;
            BTreeDelete(oldKey);
            BTreeInsert(new PxEntry(newKey, blockNumber, recordIndex));
        }

        public void OnDelete(object[] allFieldValues)
        {
            byte[] keyData = KeySerializer.Serialize(
                ExtractIndexValues(allFieldValues), indexedFields);
            BTreeDelete(keyData);
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
        // B-Tree: Insert
        // ----------------------------------------------------------------

        private void BTreeInsert(PxEntry entry)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndex.BTreeInsert] rootBlock={indexFile.pxRootBlockId}, " +
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
                var child = ReadBlock(node.Entries[i].BlockNumber);
                if (child.IsFull(entrySize))
                {
                    SplitChild(node, i, child);
                    if (CompareKeys(entry.KeyData, node.Entries[i].KeyData) > 0) i++;
                }
                InsertNonFull(ReadBlock(node.Entries[i].BlockNumber), entry);
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
        // B-Tree: Delete
        // ----------------------------------------------------------------

        private void BTreeDelete(byte[] keyData)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndex.BTreeDelete] rootBlock={indexFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(keyData)}");
            var root = ReadBlock(indexFile.pxRootBlockId);
            DeleteFromNode(root, keyData);
            if (root.Entries.Count == 0 && !root.IsLeaf)
            {
                UpdateRootBlockId(root.LeftChildBlockNumber);
                FreeBlock(root.BlockNumber);
            }
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
                r.ReadUInt16();
                int bytesRead = LEFT_CHILD_PTR_SIZE;
                while (bytesRead + entrySize <= blockCapacity)
                {
                    byte[] key = r.ReadBytes(keyDataSize);
                    if (IsEmptyKey(key)) break;
                    ushort bn = r.ReadUInt16();
                    ushort ro = r.ReadUInt16();
                    block.Entries.Add(new PxEntry(key, bn, ro));
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
                w.Write(block.LeftChildBlockNumber);
                w.Write((ushort)0);
                foreach (var e in block.Entries)
                {
                    w.Write(e.KeyData);
                    w.Write(e.BlockNumber);
                    w.Write(e.RecordOffset);
                }
                int used = LEFT_CHILD_PTR_SIZE + block.UsedSize;
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
                $"[SecondaryIndex.AllocateBlock] Allocated block {n} at " +
                $"offset={indexFile.headerSize + (long)(n - blockBase) * blockSize}, newStreamLength={indexFile.stream.Length}");
            return new PxBlock { BlockNumber = n, Capacity = blockCapacity };
        }

        private void FreeBlock(ushort blockNumber)
        {
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos       = indexFile.headerSize + (long)(blockNumber - blockBase) * blockSize;
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndex.FreeBlock] Zeroing block {blockNumber} at offset={pos}");
            indexFile.stream.Position = pos;
            indexFile.stream.Write(new byte[blockSize], 0, blockSize);
        }

        private void UpdateRootBlockId(ushort newRootId)
        {
            indexFile.stream.Position = 0x1E;
            using (var w = new BinaryWriter(indexFile.stream, Encoding.Default, leaveOpen: true))
                w.Write(newRootId);
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

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose() => indexFile?.Dispose();
    }
}