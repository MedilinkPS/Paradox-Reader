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
            : this(new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
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
        /// Overwrites an existing blob slot in the .MB file with <paramref name="blobVal"/>.
        /// This is a targeted, minimal write:
        ///  - For same-size data: seeks directly to the data position (offset+hsize) and
        ///    writes only the payload bytes.  The header is never touched.
        ///  - For different-size data: additionally patches only the 4-byte checkSize field
        ///    inside the header via a separate targeted seek.
        /// Type-3 (sub-block / Graphics) blobs update just the 5-byte blob pointer and data.
        /// </summary>
        internal void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {
            if (blobVal == null || blobVal.Length == 0)
                throw new ArgumentException("Blob value cannot be null or empty.");

            var leader  = len - 10;
            var newSize = (uint)blobVal.Length;
            var index   = BitConverter.ToUInt32(blobInfo, leader) & 0x000000ff;
            var offset  = BitConverter.ToUInt32(blobInfo, leader) & 0xffffff00;
            var oldSize = BitConverter.ToUInt32(blobInfo, leader + 4);

            // Read only the type byte — the minimal peek needed to choose the write path.
            this.stream.Position = offset;
            int typeByte = this.stream.ReadByte();

            if (typeByte == 3)
            {
                // Paradox type-3 sub-block memo strategy:
                //   NEVER overwrite in-place. Always allocate a NEW slot (one below the
                //   current lowest-indexed active slot) and FREE the old slot (zero ptr[0]
                //   and ptr[2]).  The DB blobInfo slot-index byte is updated to the new slot.
                //
                // Slot entry layout (5 bytes):
                //   [0] dataOffsetMultiplier → data at blockBase + [0]*16
                //   [1] n  → checkSize = (n-1)*16 + [4]
                //   [2] global write-sequence counter (unique per-slot, monotonically increasing)
                //   [3] 0
                //   [4] remainder

                long slotTableBase = (long)offset + 12;

                // Scan all slots 0..index to find current state.
                byte maxPtr0     = 0;
                byte maxPtrSeq   = 0;
                int  minActiveSlot = (int)index; // start at current slot; will walk down
                byte[] scanBuf   = new byte[5];
                for (int si = 0; si <= (int)index; si++)
                {
                    long sp = slotTableBase + si * 5;
                    if (sp + 5 > this.stream.Length) break;
                    this.stream.Position = sp;
                    int sr = 0;
                    while (sr < 5) sr += this.stream.Read(scanBuf, sr, 5 - sr);
                    if (scanBuf[0] != 0) // active slot
                    {
                        if (si < minActiveSlot) minActiveSlot = si;
                        if (scanBuf[0] > maxPtr0)   maxPtr0   = scanBuf[0];
                        if (scanBuf[2] > maxPtrSeq) maxPtrSeq = scanBuf[2];
                    }
                }

                byte newSeq      = (byte)(maxPtrSeq + 1);
                byte newPtr0     = (byte)(maxPtr0   + 1);
                int  newSlotIdx  = minActiveSlot - 1; // next free slot below current lowest

                // Read old slot entry.
                long oldPtrPos = slotTableBase + (long)index * 5;
                this.stream.Position = oldPtrPos;
                byte[] oldPtr = new byte[5];
                int read = 0;
                while (read < 5) read += this.stream.Read(oldPtr, read, 5 - read);

                // Size fields may change.
                byte newN   = oldPtr[1];
                byte newRem = oldPtr[4];
                if (newSize != oldSize)
                {
                    newN   = (byte)(newSize / 16 + 1);
                    newRem = (byte)(newSize % 16);
                    Buffer.BlockCopy(BitConverter.GetBytes(newSize), 0, blobInfo, leader + 4, 4);
                }

                // Free old slot: zero ptr[0] and ptr[2].
                oldPtr[0] = 0;
                oldPtr[2] = 0;
                this.stream.Position = oldPtrPos;
                this.stream.Write(oldPtr, 0, 5);

                // Write new slot entry.
                byte[] newPtr = new byte[] { newPtr0, newN, newSeq, 0, newRem };
                long newPtrPos = slotTableBase + (long)newSlotIdx * 5;
                this.stream.Position = newPtrPos;
                this.stream.Write(newPtr, 0, 5);

                // Update blobInfo: slot-index byte (blobInfo[leader], the low byte of the
                // packed offset+index uint32) and mod_nr.
                blobInfo[leader]     = (byte)newSlotIdx;
                blobInfo[leader + 8] = newSeq;
                blobInfo[leader + 9] = 0;

                // Write payload at the new data position.
                this.stream.Position = (long)offset + newPtr0 * 16;
                this.stream.Write(blobVal, 0, blobVal.Length);

                // Update block header bytes that track allocation state.
                //
                // [3] ^= freed slot index  (XOR accumulator — records which slot was freed)
                // [4]  = freed slot index  (direct copy)
                // [5]  = (4 * newSeq * newRem - 15 * (2 * newPtr0 + 1)) mod 256
                // [6]  = new slot's ptr[0] (data block index)
                // [9]  = newSeq << 2
                // [10] = (newPtr0 + 1) - hdr[9]
                // [11] = 1 (block-modified flag)
                byte hdr5  = (byte)((4 * newSeq * newRem - 15 * (2 * newPtr0 + 1)) & 0xFF);
                byte hdr9  = (byte)(newSeq << 2);
                byte hdr10 = (byte)((newPtr0 + 1) - hdr9);

                // Read current [3] so we can XOR into it.
                this.stream.Position = (long)offset + 3;
                byte cur3 = (byte)this.stream.ReadByte();

                this.stream.Position = (long)offset + 3;
                this.stream.WriteByte((byte)(cur3 ^ (byte)index));  // [3] ^= freed slot
                this.stream.WriteByte((byte)index);                  // [4]  = freed slot
                this.stream.WriteByte(hdr5);                         // [5]  = derived formula
                this.stream.WriteByte(newPtr0);                      // [6]  = new ptr[0]
                this.stream.Position = (long)offset + 9;
                this.stream.WriteByte(hdr9);   // [9]
                this.stream.WriteByte(hdr10);  // [10]
                this.stream.WriteByte(1);      // [11]
            }
            else
            {
                // Type 2 (or any non-3 block): data lives immediately after the hsize-byte
                // header, i.e. at offset+hsize.  ReadBlob confirms this: after reading
                // 3 + (hsize-3) = hsize bytes sequentially it reads the data with no
                // intervening seek.
                //
                // We do NOT rewrite the header block — touching those bytes risks
                // corrupting structural unknowns (offset+1..2 and offset+7..9).
                // Instead we patch only the 4-byte checkSize field when the size changes,
                // using a direct targeted seek.

                if (newSize != oldSize)
                {
                    // checkSize = file[offset+3..offset+6] (LE uint32).
                    // Derived from ReadBlob: second Read puts file[offset+3..] into head[0..],
                    // and checkSize = ToUInt32(head, 0).
                    byte[] sizeBytes = BitConverter.GetBytes(newSize);
                    this.stream.Position = (long)offset + 3;
                    this.stream.Write(sizeBytes, 0, 4);
                    Buffer.BlockCopy(sizeBytes, 0, blobInfo, leader + 4, 4);
                }

                // Write the payload — mirrors the exact read position in ReadBlob.
                this.stream.Position = (long)offset + hsize;
                this.stream.Write(blobVal, 0, blobVal.Length);
            }

            // Increment the write counter stored in the BAT block at offset 3.
            // Paradox uses this single-byte counter to detect stale cached blob data.
            this.stream.Position = 3;
            int batCounter = this.stream.ReadByte();
            this.stream.Position = 3;
            this.stream.WriteByte((byte)((batCounter + 1) & 0xFF));

            this.stream.Flush();
        }


    }

}
