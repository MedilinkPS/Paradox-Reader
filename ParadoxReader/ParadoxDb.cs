using System;
using System.IO;
using System.Text;
using DbBlock = ParadoxReader.DataBlock; // Alias to avoid conflict with ParadoxFile.DataBlock

namespace ParadoxReader
{
    /// <summary>
    /// Extends ParadoxFile with full read/write support, including
    /// automatic maintenance of .PX and .Xnn index files and
    /// BDE-compatible file locking via .LCK files.
    /// </summary>
    public class ParadoxDb : ParadoxFile
    {
        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly string          fileName;
        private readonly BlockManager    blockManager;
        private readonly IndexManager    indexManager;
        private readonly ParadoxFileLock fileLock;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public ParadoxDb(string fileName) : base(fileName)
        {
            this.fileName = fileName;

            blockManager = new BlockManager(
                stream,
                headerSize,
                RecordSize,
                maxTableSize);

            indexManager = new IndexManager(
                fileName,
                FieldTypes,
                primaryKeyFields);

            fileLock = new ParadoxFileLock(fileName);
        }

        // ----------------------------------------------------------------
        // Insert
        // ----------------------------------------------------------------

        /// <summary>
        /// Inserts a new record and updates all index files.
        /// </summary>
        /// <param name="fieldValues">Values for all fields, in field order.</param>
        /// <returns>The newly inserted record including its block/record position.</returns>
        public ParadoxReader.ParadoxRecord InsertRecord(object[] fieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                byte[]   recordBytes = SerializeRecord(fieldValues);
                DbBlock  block       = FindOrAllocateBlockForInsert();
                ushort   recIdx      = (ushort)block.RecordCount;

                block.SetRecordBytes(recIdx, recordBytes);
                block.RecordCount++;
                blockManager.WriteBlock(block);

                RecordCount++;
                WriteRecordCountToHeader();

                indexManager.OnInsert(fieldValues, block.BlockNumber, recIdx);
                IncrementChangeCount();

                return new ParadoxRecord(block.BlockNumber, recIdx, fieldValues);
            }
        }

        // ----------------------------------------------------------------
        // Update
        // ----------------------------------------------------------------

        /// <summary>
        /// Updates an existing record in place and updates all index files.
        /// </summary>
        public void UpdateRecord(ParadoxReader.ParadoxRecord existing, object[] newFieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                byte[]  recordBytes = SerializeRecord(newFieldValues);
                DbBlock block       = blockManager.ReadBlock(existing.BlockNumber);

                block.SetRecordBytes(existing.RecordIndex, recordBytes);
                blockManager.WriteBlock(block);

                indexManager.OnUpdate(
                    existing.DataValues, newFieldValues,
                    existing.BlockNumber, existing.RecordIndex);

                IncrementChangeCount();

                // Refresh cached values on the record object
                existing.DataValues = newFieldValues;
            }
        }

        // ----------------------------------------------------------------
        // Delete
        // ----------------------------------------------------------------

        /// <summary>
        /// Deletes a record and updates all index files.
        /// </summary>
        public void DeleteRecord(ParadoxReader.ParadoxRecord record)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                DbBlock block = blockManager.ReadBlock(record.BlockNumber);

                // Shift all records after the deleted one down by one slot
                for (int i = record.RecordIndex; i < block.RecordCount - 1; i++)
                    block.SetRecordBytes(i, block.GetRecordBytes(i + 1));

                // Zero out the vacated last slot
                block.SetRecordBytes(block.RecordCount - 1, new byte[RecordSize]);
                block.RecordCount--;
                blockManager.WriteBlock(block);

                RecordCount--;
                WriteRecordCountToHeader();

                indexManager.OnDelete(record.DataValues);
                IncrementChangeCount();
            }
        }

        // ----------------------------------------------------------------
        // Record serialization
        // ----------------------------------------------------------------

        private byte[] SerializeRecord(object[] fieldValues)
        {
            byte[] bytes = KeySerializer.Serialize(fieldValues, FieldTypes);
            if (bytes.Length != RecordSize)
                Array.Resize(ref bytes, RecordSize);
            return bytes;
        }

        // ----------------------------------------------------------------
        // Block management
        // ----------------------------------------------------------------

        private DbBlock FindOrAllocateBlockForInsert()
        {
            // Empty table: no blocks exist yet
            if (firstBlock == 0)
            {
                DbBlock first = blockManager.AllocateBlock(prevBlockNumber: 0);
                blockManager.WriteBlock(first);
                firstBlock = first.BlockNumber;
                lastBlock  = first.BlockNumber;
                fileBlocks = 1;
                WriteBlockHeadersToFileHeader();
                return first;
            }

            // Try the last block first (most likely to have room)
            DbBlock candidate = blockManager.ReadBlock(lastBlock);
            if (candidate.HasRoom) return candidate;

            // Walk backwards looking for any block with room
            ushort current = candidate.PrevBlock;
            while (current != 0)
            {
                DbBlock block = blockManager.ReadBlock(current);
                if (block.HasRoom) return block;
                current = block.PrevBlock;
            }

            // All blocks are full — allocate a new last block
            DbBlock newBlock = blockManager.AllocateBlock(prevBlockNumber: lastBlock);

            // Link old last → new block
            DbBlock oldLast = blockManager.ReadBlock(lastBlock);
            oldLast.NextBlock = newBlock.BlockNumber;
            blockManager.WriteBlock(oldLast);

            lastBlock  = newBlock.BlockNumber;
            fileBlocks = (ushort)(fileBlocks + 1);
            WriteBlockHeadersToFileHeader();

            return newBlock;
        }

        // ----------------------------------------------------------------
        // Header updates
        // ----------------------------------------------------------------

        /// <summary>RecordCount is at offset 0x04 (int32).</summary>
        private void WriteRecordCountToHeader()
        {
            stream.Position = 0x04;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
                w.Write(RecordCount);
        }

        /// <summary>
        /// Writes block chain fields back to the header.
        /// Layout (from ReadHeader): nextBlock(2) fileBlocks(2) firstBlock(2) lastBlock(2)
        /// starting at offset 0x08.
        /// </summary>
        private void WriteBlockHeadersToFileHeader()
        {
            stream.Position = 0x08;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
            {
                w.Write(nextBlock);
                w.Write(fileBlocks);
                w.Write(firstBlock);
                w.Write(lastBlock);
            }
        }

        /// <summary>
        /// Increments changeCount1/changeCount2 so BDE detects that
        /// the table has been modified. Offsets 0x2D and 0x2E.
        /// </summary>
        private void IncrementChangeCount()
        {
            changeCount1++;
            if (changeCount1 == 0) changeCount2++; // carry on overflow

            stream.Position = 0x2D;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
            {
                w.Write(changeCount1);
                w.Write(changeCount2);
            }
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public override void Dispose()
        {
            fileLock?.Dispose();
            indexManager?.Dispose();
            base.Dispose();
        }
    }
}