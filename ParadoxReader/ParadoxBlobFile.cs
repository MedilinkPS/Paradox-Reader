using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    internal class ParadoxBlobFile : IDisposable
    {
        private readonly Stream stream;
        private readonly BinaryReader reader;

        public ParadoxBlobFile(string fileName)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
        }

        public ParadoxBlobFile(Stream stream)
        {
            this.stream = stream;
            this.reader = new BinaryReader(stream);
        }

        public virtual void Dispose()
        {
            this.stream.Dispose();
        }

        public byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {

            var leader = len - 10;
            var size = BitConverter.ToUInt32(blobInfo, leader + 4);
            var blobsize = size;

            if (hsize == 17) // Graphics has a larger header size (8 bytes) at the expense of the blobsize.
            {
                blobsize = size - 8;
            }

            var index = BitConverter.ToUInt32(blobInfo, leader) & 0x000000ff;
            var mod_nr = BitConverter.ToUInt16(blobInfo, leader + 8);
            var offset = BitConverter.ToUInt32(blobInfo, leader) & 0xffffff00;


            if (size > 0)
            {

                this.stream.Position = offset;

                byte[] head;
                head = new byte[20];
                this.reader.Read(head, 0, 3);

                if (head[0] == 3)
                {
                    this.reader.Read(head, 0, 9); // Read remaining 9 bytes of header
                    var blobPointerPos = offset + 12 + (index * 5);
                    this.stream.Position = blobPointerPos; // Goto the blob pointer with the passed index
                    this.reader.Read(head, 0, 5); // Read the blob pointer
                    var checkSize = ((uint)head[1] - 1) * 16 + head[4];
                    if (checkSize == size)
                    {
                        byte[] buffer;
                        buffer = new byte[size];

                        var blobDataPos = offset + (head[0] * 16);
                        this.stream.Position = blobDataPos; // Goto the blob data position

                        this.reader.Read(buffer, 0, (int)size); // Or should this be blobsize? Need to test with graphic type
                        return buffer;
                    }
                }
                else //if (head[0] == 2)
                {
                    //TODO check for type 2 and index=255

                    this.reader.Read(head, 0, hsize - 3); // Read remaining 6 bytes of header
                    var checkSize = BitConverter.ToUInt32(head, 0);
                    if (checkSize == size)
                    {
                        byte[] buffer;
                        buffer = new byte[size];

                        this.reader.Read(buffer, 0, (int)size); // Or should this be blobsize? Need to test with graphic type
                        return buffer;
                    }
                }
            }
            return null;
        }



        /// <summary>
        /// TODO: This needs work. We need to pre-set the blobInfo with the correct values before writing.
        ///  For example size = BitConverter.ToUInt32(blobInfo, len - 10 + 4);
        /// </summary>
        internal void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {

            if (blobVal == null || blobVal.Length == 0)
                throw new ArgumentException("Blob value cannot be null or empty.");

            try
            {
                using (var writer = new BinaryWriter(stream))
                {
                    var leader = len - 10;
                    var size = (uint)blobVal.Length; // Size of the blob data to write
                    var blobsize = size;
                    if (hsize == 17) // Graphics: account for 8-byte extended header
                        blobsize = size - 8;

                    // Extract metadata from blobInfo
                    var index = BitConverter.ToUInt32(blobInfo, leader) & 0x000000ff;
                    var mod_nr = BitConverter.ToUInt16(blobInfo, leader + 8);
                    var offset = BitConverter.ToUInt32(blobInfo, leader) & 0xffffff00;

                    // Update blobInfo with size
                    Buffer.BlockCopy(BitConverter.GetBytes(size), 0, blobInfo, leader + 4, 4);

                    // Set stream position to offset and write header
                    this.stream.Position = offset;
                    byte[] head = new byte[20]; // Same size as in ReadBlob
                    if (hsize == 17) // Graphics (type 3)
                    {
                        head[0] = 3; // Type 3
                        writer.Write(head, 0, 3); // Write first 3 bytes of header
                        writer.Write(new byte[9]); // Write 9 bytes of header (placeholder, adjust if specific data needed)

                        // Compute blob pointer position
                        var blobPointerPos = offset + 12 + (index * 5);
                        this.stream.Position = blobPointerPos;

                        // Create blob pointer: head[0] = multiplier, head[1] = n, head[4] = remainder
                        // checkSize = ((head[1] - 1) * 16 + head[4]) == size
                        uint n = size / 16 + 1; // head[1] value
                        uint remainder = size % 16; // head[4] value
                        head[0] = 0; // Multiplier (adjust if needed, not specified in ReadBlob)
                        head[1] = (byte)n;
                        head[4] = (byte)remainder;
                        writer.Write(head, 0, 5); // Write 5-byte blob pointer

                        // Write blob data
                        var blobDataPos = offset + (head[0] * 16);
                        this.stream.Position = blobDataPos;
                        writer.Write(blobVal, 0, blobVal.Length);
                    }
                    else // Type 2
                    {
                        head[0] = 2; // Type 2
                        writer.Write(head, 0, 3); // Write first 3 bytes of header
                        Buffer.BlockCopy(BitConverter.GetBytes(size), 0, head, 0, 4); // Encode checkSize
                        writer.Write(head, 0, hsize - 3); // Write remaining header bytes

                        // TODO: Handle index == 255 if special logic is needed
                        writer.Write(blobVal, 0, blobVal.Length); // Write blob data
                    }
                }
            }
            catch
            {
                throw new Exception("Failed to write blob data.");
            }
        }


    }

}
