using System;
using System.IO;
using System.Text;
using DbBlock = ParadoxReader.DataBlock; // Alias to avoid conflict with ParadoxFile.DataBlock

namespace ParadoxReader
{
    /// <summary>
    /// Extends ParadoxFile with full read/write support, including
    /// automatic maintenance of .PX and .Xnn index files, .MB blob file
    /// read/write support, and BDE-compatible file locking via .LCK files.
    /// This is the single class intended for both reading and writing
    /// Paradox tables (superseding the old read-only ParadoxTable).
    /// </summary>
    public class ParadoxTableFile : ParadoxFile
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        const string DOT_DB   = ".DB";  // Data file
        const string DOT_PX   = ".PX";  // Primary Key file
        const string DOT_MB   = ".MB";  // Blob file
        const string DOT_WILD = ".*";

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly string          fileName;
        private readonly BlockManager    blockManager;
        private readonly IndexManager    indexManager;
        private readonly ParadoxFileLock fileLock;

        public ParadoxPrimaryKey PrimaryKeyIndex;
        private ParadoxBlobFile  BlobFile;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public ParadoxTableFile(string fileName) : base(fileName)
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

            DiscoverAssociatedFiles(fileName);
        }

        /// <summary>
        /// Convenience constructor matching the old ParadoxTable(dbPath, tableName) signature.
        /// </summary>
        public ParadoxTableFile(string dbPath, string tableName)
            : this(Path.Combine(dbPath, tableName?.EnsureEndsWith(DOT_DB)))
        {
        }

        /// <summary>
        /// Locates and opens the associated .PX (primary key index) and
        /// .MB (blob/memo) files alongside the .DB file, if present.
        /// </summary>
        private void DiscoverAssociatedFiles(string dbFilePath)
        {
            var dbPath              = Path.GetDirectoryName(dbFilePath);
            var tableNameWithExt    = Path.GetFileName(dbFilePath);
            var tableNameWithoutExt = Path.GetFileNameWithoutExtension(tableNameWithExt);
            var files               = Directory.GetFiles(dbPath, tableNameWithoutExt + DOT_WILD);

            foreach (var file in files)
            {
                if (Path.GetFileName(file).Equals(tableNameWithExt, StringComparison.OrdinalIgnoreCase))
                    continue; // current file

                if (Path.GetFileNameWithoutExtension(file).EndsWith(DOT_PX, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetExtension(file).Equals(DOT_PX, StringComparison.OrdinalIgnoreCase))
                {
                    this.PrimaryKeyIndex = new ParadoxPrimaryKey(this, file);
                    //break; // I'm not sure we can guarantee that PX will be found after MB.
                }
                if (Path.GetFileNameWithoutExtension(file).EndsWith(DOT_MB, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetExtension(file).Equals(DOT_MB, StringComparison.OrdinalIgnoreCase))
                {
                    this.BlobFile = new ParadoxBlobFile(file);
                }
            }
        }

        // ----------------------------------------------------------------
        // Blob file support
        // ----------------------------------------------------------------

        internal override byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {
            if (this.BlobFile == null)
            {
                return base.ReadBlob(blobInfo, len, hsize);
            }
            else
            {
                return this.BlobFile.ReadBlob(blobInfo, len, hsize);
            }
        }

        internal override void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {
            if (this.BlobFile == null)
            {
                base.WriteBlob(blobInfo, len, hsize, blobVal);
            }
            else
            {
                this.BlobFile.WriteBlob(blobInfo, len, hsize, blobVal);
                EnsureHasBlobFlagSet();
            }
        }

        /// <summary>
        /// BDE sets a flag byte at .DB header offset 0x74 to 0x01 once the table has
        /// an associated (non-empty) blob/memo file. Files created before any memo
        /// value was ever written leave this byte 0x00; without it, BDE reports the
        /// memo/blob file as corrupt even though the data itself is well-formed.
        /// </summary>
        private void EnsureHasBlobFlagSet()
        {
            const long HasBlobFlagOffset = 0x74;
            this.stream.Position = HasBlobFlagOffset;
            int current = this.stream.ReadByte();
            if (current != 1)
            {
                this.stream.Position = HasBlobFlagOffset;
                this.stream.WriteByte(1);
                this.stream.Flush();
            }
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
        // Append
        // ----------------------------------------------------------------

        /// <summary>
        /// Appends a new record to the end of the table (always adds a new
        /// record rather than reusing/overwriting an existing slot), updates
        /// all index files, and manually assigns/advances any AutoInc field.
        /// </summary>
        /// <param name="fieldValues">Values for all fields, in field order. Any AutoInc field value is ignored and assigned automatically.</param>
        /// <returns>The newly appended record including its block/record position.</returns>
        public ParadoxReader.ParadoxRecord AppendRecord(object[] fieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                // Paradox manages AutoInc values itself (not via a normal index),
                // so we must assign/increment it manually before serializing.
                AssignAutoIncValues(fieldValues);

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

        /// <summary>
        /// Finds any AutoInc field(s), assigns the next value (current
        /// autoIncVal + 1) into fieldValues, and persists the new
        /// autoIncVal back to the file header.
        /// </summary>
        private void AssignAutoIncValues(object[] fieldValues)
        {
            bool assigned = false;

            for (int i = 0; i < FieldTypes.Length; i++)
            {
                if (FieldTypes[i].fType != ParadoxFieldTypes.AutoInc)
                    continue;

                autoIncVal++;
                fieldValues[i] = autoIncVal;
                assigned = true;
            }

            if (assigned)
                WriteAutoIncValToHeader();
        }

        /// <summary>AutoIncVal (longint) is at offset 0x49.</summary>
        private void WriteAutoIncValToHeader()
        {
            stream.Position = 0x49;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
                w.Write(autoIncVal);
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
            byte[] bytes = KeySerializer.Serialize(fieldValues, FieldTypes, this);
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
            if (fileBlocks == 0)
            {
                DbBlock first = blockManager.AllocateBlock();
                firstBlock = (ushort)(first.BlockNumber + 1); // header stores 1-based block numbers
                lastBlock  = (ushort)(first.BlockNumber + 1);
                fileBlocks = 1;
                WriteBlockHeadersToFileHeader();
                return first;
            }

            // firstBlock/lastBlock in the header are 1-based; BlockManager works
            // with 0-based physical block numbers.
            ushort lastBlock0Based  = (ushort)(lastBlock  - 1);
            ushort firstBlock0Based = (ushort)(firstBlock - 1);

            // Try the last block first (most likely to have room)
            DbBlock candidate = blockManager.ReadBlock(lastBlock0Based);
            if (candidate.HasRoom) return candidate;

            // Paradox blocks only chain forward (NextBlock) — no back-pointer
            // exists, so walk forward from the first block looking for room.
            ushort current = firstBlock0Based;
            while (current != lastBlock0Based)
            {
                DbBlock block = blockManager.ReadBlock(current);
                if (block.HasRoom) return block;
                current = block.NextBlock;
            }

            // All blocks are full — allocate a new last block
            DbBlock newBlock = blockManager.AllocateBlock();

            // Link old last → new block
            DbBlock oldLast = blockManager.ReadBlock(lastBlock0Based);
            oldLast.NextBlock = newBlock.BlockNumber;
            blockManager.WriteBlock(oldLast);

            lastBlock  = (ushort)(newBlock.BlockNumber + 1); // header stores 1-based
            fileBlocks = (ushort)(fileBlocks + 1);
            WriteBlockHeadersToFileHeader();

            return newBlock;
        }

        // ----------------------------------------------------------------
        // Header updates
        // ----------------------------------------------------------------

        /// <summary>RecordCount is at offset 0x06 (int32), after RecordSize(2)+headerSize(2)+FileType(1)+maxTableSize(1).</summary>
        private void WriteRecordCountToHeader()
        {
            stream.Position = 0x06;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
                w.Write(RecordCount);
        }

        /// <summary>
        /// Writes block chain fields back to the header.
        /// Layout (from ReadHeader): RecordSize(2)@0x00, headerSize(2)@0x02,
        /// FileType(1)@0x04, maxTableSize(1)@0x05, RecordCount(4)@0x06, then
        /// nextBlock(2) fileBlocks(2) firstBlock(2) lastBlock(2) starting at 0x0A.
        /// </summary>
        private void WriteBlockHeadersToFileHeader()
        {
            stream.Position = 0x0A;
            using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
            {
                w.Write(nextBlock);
                w.Write(fileBlocks);
                w.Write(firstBlock);
                w.Write(lastBlock);
            }

            // maxBlocks (word @ 0x3A) tracks the allocated capacity of the block
            // chain. BDE/Pdxrbld flags the table as corrupt ("Total blocks greater
            // than max blocks") if fileBlocks ever exceeds it, so keep it in sync.
            if (fileBlocks > maxBlocks)
            {
                maxBlocks = fileBlocks;
                stream.Position = 0x3A;
                using (var w = new BinaryWriter(stream, Encoding.Default, leaveOpen: true))
                    w.Write(maxBlocks);
            }
        }

        /// <summary>
        /// Increments changeCount1/changeCount2 so BDE detects that
        /// the table has been modified. Offsets 0x2D and 0x2E.
        /// Also mirrors the new value into every open index file so BDE
        /// doesn't consider them out of date relative to the .DB file.
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

            indexManager.SyncChangeCount(changeCount1, changeCount2);
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public override void Dispose()
        {
            fileLock?.Dispose();
            indexManager?.Dispose();
            PrimaryKeyIndex?.Dispose();
            BlobFile?.Dispose();
            base.Dispose();
        }
    }
}