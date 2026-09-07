using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Builds brand-new Paradox .DB/.PX header + field-definition bytes from
    /// a <see cref="TableSchemaDefinition"/>, mirroring the exact byte layout
    /// <see cref="ParadoxFile.ReadHeader"/> expects to read back. This is the
    /// one place responsible for synthesizing a table's structure "from
    /// nothing" (used by both <see cref="TableCreator"/> for File > New >
    /// Table, and <see cref="TableRebuilder.RebuildWithSchema"/> for Table >
    /// Modify Structure, which regenerates a table under a new schema).
    /// </summary>
    /// <remarks>
    /// Reserved/unknown header fields (pointers, workspace fields, etc.) are
    /// written as zero - they are never dereferenced by this library, only
    /// skipped over positionally by <see cref="ParadoxFile.ReadHeader"/>, so
    /// zero is a safe, valid value for a freshly created file.
    /// </remarks>
    internal static class ParadoxHeaderBuilder
    {
        private const byte FileVersionId = 0x0C; // modern (long table-name) layout
        private const byte DefaultMaxTableSize = 1; // 1024-byte blocks
        private const int V4HeaderSize = 32;

        /// <summary>
        /// Builds a complete empty (zero-record) .DB file header + field
        /// definitions + field names for <paramref name="schema"/>.
        /// </summary>
        public static byte[] BuildDbHeader(TableSchemaDefinition schema)
        {
            var fileType = schema.PrimaryKeyFieldCount > 0
                ? ParadoxFileType.DbFileIndexed
                : ParadoxFileType.DbFileNotIndexed;

            return BuildHeader(schema, fileType, schema.Fields, includeFieldNames: true);
        }

        /// <summary>
        /// Builds a complete empty (zero-record, zero-level) .PX primary
        /// index file header + field definitions for the primary-key fields
        /// of <paramref name="schema"/>. Only valid when
        /// <see cref="TableSchemaDefinition.PrimaryKeyFieldCount"/> is > 0.
        /// </summary>
        public static byte[] BuildPxHeader(TableSchemaDefinition schema)
        {
            var keyFields = schema.Fields.Where(f => f.IsPrimaryKey).ToList();
            return BuildHeader(schema, ParadoxFileType.PxFile, keyFields, includeFieldNames: false);
        }

        /// <summary>
        /// Builds a complete empty (zero-record, one empty root block) .Xnn
        /// secondary index file header + field definitions for
        /// <paramref name="indexedFields"/> (mirroring the on-disk field
        /// composition Paradox uses: indexed field(s) followed by the
        /// table's primary-key field(s)).
        /// </summary>
        public static byte[] BuildSecondaryIndexHeader(TableSchemaDefinition schema, TableIndexDefinition index)
        {
            var fields = new List<TableFieldDefinition>();
            foreach (var i in index.FieldIndices)
                fields.Add(schema.Fields[i]);

            foreach (var f in schema.Fields.Where(f => f.IsPrimaryKey))
            {
                if (!fields.Contains(f))
                    fields.Add(f);
            }

            // No root block is pre-allocated: SecondaryIndexFile.OnBlockChanged
            // already detects an empty index (stream.Length <= headerSize ||
            // RecordCount <= 0) and allocates the first leaf block itself on
            // the very first insert, exactly as it does for a freshly
            // rebuilt/cloned index skeleton (see TableRebuilder).
            return BuildHeader(schema, ParadoxFileType.XnnFileNonInc, fields, includeFieldNames: false);
        }

        private static byte[] BuildHeader(
            TableSchemaDefinition schema,
            ParadoxFileType fileType,
            List<TableFieldDefinition> fields,
            bool includeFieldNames)
        {
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.ASCII))
            {
                int recordSize = fields.Sum(f => (int)f.Size);
                int fieldCount = fields.Count;
                int primaryKeyCount = fields.Count(f => f.IsPrimaryKey);
                int nameFieldLength = FileVersionId >= 0x0C ? 261 : 79;

                w.Write((ushort)recordSize);       // 0x00 RecordSize
                w.Write((ushort)0);                 // 0x02 headerSize (patched below)
                w.Write((byte)fileType);            // 0x04 FileType
                w.Write(DefaultMaxTableSize);        // 0x05 maxTableSize
                w.Write(0);                          // 0x06 RecordCount
                w.Write((ushort)0);                  // 0x0A nextBlock
                w.Write((ushort)0);                  // 0x0C fileBlocks
                w.Write((ushort)0);                  // 0x0E firstBlock
                w.Write((ushort)0);                  // 0x10 lastBlock
                w.Write((ushort)0);                  // 0x12 unknown12x13
                w.Write((byte)0);                    // 0x14 modifiedFlags1
                w.Write((byte)(primaryKeyCount > 0 ? 1 : 0)); // 0x15 indexFieldNumber
                w.Write(0);                          // 0x16 primaryIndexWorkspace
                w.Write(0);                          // 0x1A unknownPtr1A
                w.Write((ushort)0);                  // 0x1E pxRootBlockId
                w.Write((byte)0);                    // 0x20 pxLevelCount
                w.Write((short)fieldCount);          // 0x21 FieldCount
                w.Write((short)primaryKeyCount);     // 0x23 primaryKeyFields
                w.Write(0);                          // 0x25 encryption1
                w.Write((byte)0);                    // 0x29 sortOrder
                w.Write((byte)0);                    // 0x2A modifiedFlags2
                w.Write(new byte[2]);                 // 0x2B-0x2C unknown2Bx2C
                w.Write((byte)0);                    // 0x2D changeCount1
                w.Write((byte)0);                    // 0x2E changeCount2
                w.Write((byte)0);                    // 0x2F unknown2F
                w.Write(0);                          // 0x30 tableNamePtrPtr
                w.Write(0);                          // 0x34 fldInfoPtr
                w.Write((byte)0);                    // 0x38 writeProtected
                w.Write(FileVersionId);               // 0x39 fileVersionID
                w.Write((ushort)0);                  // 0x3A maxBlocks
                w.Write((byte)0);                    // 0x3C unknown3C
                w.Write((byte)0);                    // 0x3D auxPasswords
                w.Write(new byte[2]);                 // 0x3E-0x3F unknown3Ex3F
                w.Write(0);                          // 0x40 cryptInfoStartPtr
                w.Write(0);                          // 0x44 cryptInfoEndPtr
                w.Write((byte)0);                    // 0x48 unknown48
                w.Write(0);                          // 0x49 autoIncVal
                w.Write(new byte[2]);                 // 0x4D-0x4E unknown4Dx4E
                w.Write((byte)0);                    // 0x4F indexUpdateRequired
                w.Write(new byte[5]);                 // 0x50-0x54 unknown50x54
                w.Write((byte)0);                    // 0x55 refIntegrity
                w.Write(new byte[2]);                 // 0x56-0x57 unknown56x57

                // V4 header (only DB/index file types, fileVersionID >= 5 - always true here)
                w.Write((short)0);   // fileVerID2
                w.Write((short)0);   // fileVerID3
                w.Write(0);          // encryption2
                w.Write(0);          // fileUpdateTime
                w.Write((ushort)0);  // hiFieldID
                w.Write((ushort)0);  // hiFieldIDinfo
                w.Write((short)0);   // sometimesNumFields
                w.Write((ushort)0);  // dosCodePage
                w.Write(new byte[4]); // unknown6Cx6F
                w.Write((short)0);   // changeCount4
                w.Write(new byte[6]); // unknown72x77

                // Field definitions (fType byte + fSize byte per field)
                foreach (var f in fields)
                {
                    w.Write((byte)f.Type);
                    w.Write(f.Size);
                }

                w.Write(0); // tableNamePtr

                if (includeFieldNames)
                {
                    // fieldNamePtrArray (one int32 per field - unused positions, zero is fine)
                    for (int i = 0; i < fieldCount; i++)
                        w.Write(0);
                }

                // Table name (null-terminated ASCII, padded to nameFieldLength)
                var tableNameBytes = new byte[nameFieldLength];
                var nameBytes = Encoding.ASCII.GetBytes(schema.TableName ?? string.Empty);
                Array.Copy(nameBytes, tableNameBytes, Math.Min(nameBytes.Length, nameFieldLength - 1));
                w.Write(tableNameBytes);

                if (includeFieldNames)
                {
                    foreach (var f in fields)
                    {
                        var fieldNameBytes = Encoding.ASCII.GetBytes(f.Name ?? string.Empty);
                        w.Write(fieldNameBytes);
                        w.Write((byte)0);
                    }
                }

                w.Flush();
                byte[] header = ms.ToArray();

                // Patch headerSize now that the true length is known.
                byte[] headerSizeBytes = BitConverter.GetBytes((ushort)header.Length);
                header[ParadoxHeaderOffsets.HeaderSize] = headerSizeBytes[0];
                header[ParadoxHeaderOffsets.HeaderSize + 1] = headerSizeBytes[1];

                return header;
            }
        }
    }
}
