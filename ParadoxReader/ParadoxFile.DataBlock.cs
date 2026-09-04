using System;
using System.IO;
using System.Linq;

namespace ParadoxReader
{
    public partial class ParadoxFile
    {
        internal class DataBlock
        {
            public ParadoxFile file;
            ushort nextBlock;
            internal ushort blockNumber;
            short addDataSize;
            public byte[] data;
            private ParadoxReader.ParadoxRecord[] recCache;

            public int RecordCount { get; private set; }

            public DataBlock(ParadoxFile file, BinaryReader reader, ushort? expectedBlockNumber = null)
            {
                this.file = file;
                this.nextBlock = reader.ReadUInt16();
                this.blockNumber = reader.ReadUInt16();
                this.addDataSize = reader.ReadInt16();

                // This is kind of unnecessary but I wanted to double check we were getting the correct blockNumber
                if(expectedBlockNumber.HasValue && this.blockNumber != expectedBlockNumber)
                {
                    throw new Exception($"Expected block number {expectedBlockNumber} but got {this.blockNumber}");
                }

                var recordCount = (addDataSize / (this.file.RecordSize)) + 1;
                this.RecordCount = recordCount;
                var recordCountBySize = this.RecordCount * (this.file.RecordSize);
                this.data = reader.ReadBytes(recordCountBySize);
                this.recCache = new ParadoxReader.ParadoxRecord[this.data.Length];
            }

            public ParadoxReader.ParadoxRecord this[int recIndex]
            {
                get
                {
                    if (this.recCache[recIndex] == null)
                    {
                        this.recCache[recIndex] = new ParadoxReader.ParadoxRecord(this, recIndex);
                    }
                    return this.recCache[recIndex];
                }
            }

            internal void WriteRecordToFile(int recIndex)
            {
                file.WriteRecords(this.data, this.blockNumber, new[] { recIndex } );
            }

            internal void WriteRecordsToFile()
            {
                file.WriteRecords(this.data, this.blockNumber, Enumerable.Range(0, this.data.Length).ToArray());
            }
        }
    }
}
