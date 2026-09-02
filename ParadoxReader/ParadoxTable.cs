using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    public class ParadoxTable : ParadoxFile
    {
        public readonly ParadoxPrimaryKey PrimaryKeyIndex;
        private readonly ParadoxBlobFile BlobFile;

        const string DOT_DB = ".DB"; // Data file
        const string DOT_PX = ".PX"; // Primary Key file
        const string DOT_MB = ".MB"; // Blob file
        const string DOT_WILD = ".*";

        public ParadoxTable(string dbPath, string tableName)
            : base(Path.Combine(dbPath, tableName?.EnsureEndsWith(DOT_DB)))
        {
            var tableNameWithExt = tableName?.EnsureEndsWith(DOT_DB);
            var tableNameWithoutExt = Path.GetFileNameWithoutExtension(tableNameWithExt);
            var files = Directory.GetFiles(dbPath, tableNameWithoutExt + DOT_WILD);
            foreach (var file in files)
            {
                if (Path.GetFileName(file) == tableNameWithExt) continue; // current file
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

        public override void Dispose()
        {
            base.Dispose();
            if (this.PrimaryKeyIndex != null)
            {
                this.PrimaryKeyIndex.Dispose();
            }
            if (this.BlobFile != null)
            {
                this.BlobFile.Dispose();
            }
        }
    }
}
