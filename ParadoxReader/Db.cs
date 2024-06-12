using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;

namespace ParadoxReader
{

    public enum ParadoxFieldTypes : byte
    {
        Alpha = 0x01,
        Date = 0x02,
        Short = 0x03,
        Long = 0x04,
        Currency = 0x05,
        Number = 0x06,
        Logical = 0x09,
        MemoBLOb = 0x0C,
        BLOb = 0x0D,
        FmtMemoBLOb = 0x0E,
        OLE = 0x0F,
        Graphic = 0x10,
        Time = 0x14,
        Timestamp = 0x15,
        AutoInc = 0x16,
        BCD = 0x17,
        Bytes = 0x18
    }

    public enum ParadoxFileType : byte
    {
        DbFileIndexed = 0,
        PxFile = 1,
        DbFileNotIndexed = 2,
        XnnFileNonInc = 3,
        YnnFile = 4,
        XnnFileInc = 5,
        XgnFileNonInc = 6,
        YgnFile = 7,
        XgnFileInc = 8
    }

    public class ParadoxFile : IDisposable
    {
        public string TableName;

        public ushort RecordSize { get; private set; }
        ushort headerSize;
        public ParadoxFileType FileType { get; private set; }
        byte maxTableSize;
        public int RecordCount { get; private set; }
        ushort nextBlock;
        ushort fileBlocks;
        ushort firstBlock;
        ushort lastBlock;
        ushort unknown12x13;
        byte modifiedFlags1;
        byte indexFieldNumber;
        int primaryIndexWorkspace;
        int unknownPtr1A;
        protected ushort pxRootBlockId;
        protected byte pxLevelCount;
        public short FieldCount { get; private set; }
        short primaryKeyFields;
        int encryption1;
        byte sortOrder;
        byte modifiedFlags2;
        private byte[] unknown2Bx2C;  //  array[$002B..$002C] of byte;
        byte changeCount1;
        byte changeCount2;
        byte unknown2F;
        private int tableNamePtrPtr; // ^pchar;
        private int fldInfoPtr;  //  PFldInfoRec;
        byte writeProtected;
        byte fileVersionID;
        ushort maxBlocks;
        byte unknown3C;
        byte auxPasswords;
        private byte[] unknown3Ex3F; //  array[$003E..$003F] of byte;
        private int cryptInfoStartPtr; //  pointer;
        int cryptInfoEndPtr;
        byte unknown48;
        private int autoIncVal; //  longint;
        private byte[] unknown4Dx4E;  //array[$004D..$004E] of byte;
        byte indexUpdateRequired;
        byte[] unknown50x54;  //array[$0050..$0054] of byte;
        private byte refIntegrity;
        byte[] unknown56x57;  //array[$0056..$0057] of byte;
        private V4Hdr V4Header;
        internal FieldInfo[] FieldTypes { get; set; } // array[1..255] of TFldInfoRec);
        private int tableNamePtr;
        private int[] fieldNamePtrArray;
        public string[] FieldNames { get; private set; }

        private readonly Stream stream;
        private readonly BinaryReader reader;

        public ParadoxFile(string fileName) : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
        }

        public ParadoxFile(Stream stream)
        {
            this.stream = stream;
            this.reader = new BinaryReader(stream);
            stream.Position = 0;
            this.ReadHeader();
        }

        public virtual void Dispose()
        {
            this.stream.Dispose();
        }

        internal virtual byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {
            return null;
        }

        public IEnumerable<ParadoxRecord> Enumerate(Predicate<ParadoxRecord> where = null)
        {
            for (int blockId = 0; blockId < this.fileBlocks; blockId++)
            {
                var block = this.GetBlock(blockId);
                for (var recId = 0; recId < block.RecordCount; recId++)
                {
                    var rec = block[recId];
                    if (where == null || where(rec))
                    {
                        yield return rec;
                    }
                }
            }
        }

        private void ReadHeader()
        {
            var r = this.reader;
            RecordSize = r.ReadUInt16();
            headerSize = r.ReadUInt16();
            FileType = (ParadoxFileType) r.ReadByte();
            maxTableSize = r.ReadByte();
            RecordCount = r.ReadInt32();
            nextBlock = r.ReadUInt16();
            fileBlocks = r.ReadUInt16();
            firstBlock = r.ReadUInt16();
            lastBlock = r.ReadUInt16();
            unknown12x13 = r.ReadUInt16();
            modifiedFlags1 = r.ReadByte();
            indexFieldNumber = r.ReadByte();
            primaryIndexWorkspace = r.ReadInt32();
            unknownPtr1A = r.ReadInt32();
            pxRootBlockId = r.ReadUInt16();
            pxLevelCount = r.ReadByte();
            FieldCount = r.ReadInt16();
            primaryKeyFields = r.ReadInt16();
            encryption1 = r.ReadInt32();
            sortOrder = r.ReadByte();
            modifiedFlags2 = r.ReadByte();
            unknown2Bx2C = r.ReadBytes(0x002C - 0x002B + 1);
            changeCount1 = r.ReadByte();
            changeCount2 = r.ReadByte();
            unknown2F = r.ReadByte();
            tableNamePtrPtr = r.ReadInt32(); // ^pchar;
            fldInfoPtr = r.ReadInt32(); //  PFldInfoRec;
            writeProtected = r.ReadByte();
            fileVersionID = r.ReadByte();
            maxBlocks = r.ReadUInt16();
            unknown3C = r.ReadByte();
            auxPasswords = r.ReadByte();
            unknown3Ex3F = r.ReadBytes(0x003F - 0x003E + 1);
            cryptInfoStartPtr = r.ReadInt32(); //  pointer;
            cryptInfoEndPtr = r.ReadInt32();
            unknown48 = r.ReadByte();
            autoIncVal = r.ReadInt32(); //  longint;
            unknown4Dx4E = r.ReadBytes(0x004E - 0x004D + 1);
            indexUpdateRequired = r.ReadByte();
            unknown50x54 = r.ReadBytes(0x0054 - 0x0050 + 1);
            refIntegrity = r.ReadByte();
            unknown56x57 = r.ReadBytes(0x0057 - 0x0056 + 1);

            if ((this.FileType == ParadoxFileType.DbFileIndexed ||
                 this.FileType == ParadoxFileType.DbFileNotIndexed ||
                 this.FileType == ParadoxFileType.XnnFileInc ||
                 this.FileType == ParadoxFileType.XnnFileNonInc) &&
                this.fileVersionID >= 5)
            {
                this.V4Header = new V4Hdr(r);
            }
            var buff = new List<FieldInfo>();
            for (int i = 0; i < this.FieldCount; i++)
            {
                buff.Add(new FieldInfo(r));
            }
            if (this.FileType == ParadoxFileType.PxFile)
            {
                this.FieldCount += 3;
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
            }
            this.FieldTypes = buff.ToArray();
            this.tableNamePtr = r.ReadInt32();
            if (this.FileType == ParadoxFileType.DbFileIndexed ||
                this.FileType == ParadoxFileType.DbFileNotIndexed)
            {
                fieldNamePtrArray = new int[this.FieldCount];
                for (int i = 0; i < this.FieldCount; i++)
                {
                    this.fieldNamePtrArray[i] = r.ReadInt32();
                }
            }
            var tableNameBuff = r.ReadBytes(this.fileVersionID >= 0x0C ? 261 : 79);
            this.TableName = Encoding.ASCII.GetString(tableNameBuff, 0, Array.FindIndex(tableNameBuff, b => b == 0));
            if (this.FileType == ParadoxFileType.DbFileIndexed ||
                this.FileType == ParadoxFileType.DbFileNotIndexed)
            {
                FieldNames = new string[this.FieldCount];
                for (int i = 0; i < this.FieldCount; i++)
                {
                    var fldNameBuff = new StringBuilder();
                    char ch;
                    while ((ch = r.ReadChar()) != '\x00') fldNameBuff.Append(ch);
                    this.FieldNames[i] = fldNameBuff.ToString();
                }
            }
        }

        internal DataBlock GetBlock(int blockId)
        {
            this.stream.Position = blockId * this.maxTableSize * 0x0400 + this.headerSize;
            return new DataBlock(this, this.reader);
        }

        public string GetString(byte[] data, int from, int maxLength)
        {
            int dataLength = data.Length;
            int stringLength = Array.FindIndex(data, from, b => b == 0) - from;
            if (stringLength > maxLength)
                stringLength = maxLength;
            if (stringLength < 0)
                stringLength = 0;
            if (from < 0)
                from = 0;
            if ((from + stringLength) > dataLength)
                stringLength = dataLength;
            return Encoding.Default.GetString(data, from, stringLength);
        }

        public string GetStringFromMemo(byte[] data, int from, int size)
        {
            var memoBufferSize = size - 10;
            var memoDataBuffer = new byte[memoBufferSize];
            var memoMetaData = new byte[10];
            Array.Copy(data, from, memoDataBuffer, 0, memoBufferSize);
            Array.Copy(data, from + memoBufferSize, memoMetaData, 0, 10);

            //var offsetIntoMemoFile = (long)BitConverter.ToInt32(memoMetaData, 0); 
            //offsetIntoMemoFile &= 0xffffff00;
            //var memoModNumber = BitConverter.ToInt16(memoMetaData,8); 
            //var index = memoMetaData[0]; 

            var memoSize = BitConverter.ToInt32(memoMetaData, 4);
            return GetString(memoDataBuffer, 0, memoSize);
        }

        public class V4Hdr
        {
            short fileVerID2;
            short fileVerID3;
            int encryption2;
            int fileUpdateTime;  // 4.0 only
            ushort hiFieldID;
            ushort hiFieldIDinfo;
            short sometimesNumFields;
            ushort dosCodePage;
            private byte[] unknown6Cx6F;  //array[$006C..$006F] of byte;
            private short changeCount4;
            private byte[] unknown72x77; //    :  array[$0072..$0077] of byte;

            public V4Hdr(BinaryReader r)
            {
                fileVerID2 = r.ReadInt16();
                fileVerID3 = r.ReadInt16();
                encryption2 = r.ReadInt32();
                fileUpdateTime = r.ReadInt32(); // 4.0 only
                hiFieldID = r.ReadUInt16();
                hiFieldIDinfo = r.ReadUInt16();
                sometimesNumFields = r.ReadInt16();
                dosCodePage = r.ReadUInt16();
                unknown6Cx6F = r.ReadBytes(0x006F - 0x006C + 1); //array[$006C..$006F] of byte;
                changeCount4 = r.ReadInt16();
                unknown72x77 = r.ReadBytes(0x0077 - 0x0072 + 1); //    :  array[$0072..$0077] of byte;
            }

        }

        internal class DataBlock
        {
            public ParadoxFile file;
            ushort nextBlock;
            ushort blockNumber;
            short addDataSize;
            public byte[] data;
            private ParadoxRecord[] recCache;

            public int RecordCount { get; private set; }

            public DataBlock(ParadoxFile file, BinaryReader r)
            {
                this.file = file;
                this.nextBlock = r.ReadUInt16();
                this.blockNumber = r.ReadUInt16();
                this.addDataSize = r.ReadInt16();

                this.RecordCount = (addDataSize/file.RecordSize) + 1;
                this.data = r.ReadBytes(this.RecordCount * file.RecordSize);
                this.recCache = new ParadoxRecord[this.data.Length];
            }

            public ParadoxRecord this[int recIndex]
            {
                get
                {
                    if (this.recCache[recIndex] == null)
                    {
                        this.recCache[recIndex] = new ParadoxRecord(this, recIndex);
                    }
                    return this.recCache[recIndex];
                }
            }
        }

        internal class FieldInfo
        {
            public ParadoxFieldTypes fType;
            public byte fSize;

            public FieldInfo(ParadoxFieldTypes fType, byte fSize)
            {
                this.fType = fType;
                this.fSize = fSize;
            }

            public FieldInfo(BinaryReader r)
            {
                this.fType = (ParadoxFieldTypes)r.ReadByte();
                this.fSize = r.ReadByte();
            }
        }


    }

    public class ParadoxTable : ParadoxFile
    {
        public readonly ParadoxPrimaryKey PrimaryKeyIndex;
        private readonly ParadoxBlobFile BlobFile;

        public ParadoxTable(string dbPath, string tableName) : base(Path.Combine(dbPath, tableName + ".db"))
        {
            var files = Directory.GetFiles(dbPath, tableName + "*.*");
            foreach (var file in files)
            {
                if (Path.GetFileName(file) == tableName + ".db") continue; // current file
                if (Path.GetFileNameWithoutExtension(file).EndsWith(".PX", StringComparison.InvariantCultureIgnoreCase) ||
                    Path.GetExtension(file).Equals(".PX", StringComparison.InvariantCultureIgnoreCase))
                {
                    this.PrimaryKeyIndex = new ParadoxPrimaryKey(this, file);
                    break;
                }
                if (Path.GetFileNameWithoutExtension(file).EndsWith(".MB", StringComparison.InvariantCultureIgnoreCase) ||
                    Path.GetExtension(file).Equals(".MB", StringComparison.InvariantCultureIgnoreCase))
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
            return this.BlobFile.ReadBlob(blobInfo, len, hsize);
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

    public static class BinaryReaderExtension
    {

        public static string ReadBytesIntoBase64String(this BinaryReader reader, int count, bool returnNullInsteadOfThrow = true)
        {
            string ret = null; // string.Empty;

            try
            {
                var buff = reader.ReadBytes(count);
                if ((buff?.Length ?? 0) > 0)
                {
                    ret = Convert.ToBase64String(buff);
                }
                else
                {
                    throw new Exception("Could not read bytes.");
                }
            }
            catch
            {
                if(returnNullInsteadOfThrow)
                {
                    ret = null;
                }
                else //(Exception ex)
                {
                    throw;
                }
            }

            return ret;
        }

        /// <summary>
        /// Get the decimal value of the binary coded decimal (bytes).
        /// </summary>
        /// <param name="reader">Binary (byte) reader</param>
        /// <param name="bCDDataLen">Number of bytes, 17 for BDE</param>
        /// <param name="bCDDecLen">Number of decimal places, often 2</param>
        /// <param name="checkBCDDecLen">Check to make sure the decimal places match what is encoded in BCD</param>
        /// <param name="returnMinValueInsteadOfThrow">Avoid throwing exception and instead return decimal.minvalue</param>
        /// <returns>Decimal value of the binary coded decimal</returns>
        /// <exception cref="Exception">Decimal length doesn't match, or couldn't parse the BCD string value.</exception>
        public static decimal ReadBCD(this BinaryReader reader, int bCDDataLen = 17, int bCDDecLen = 2, bool checkBCDDecLen = false, bool returnMinValueInsteadOfThrow = true)
        {

            var ret = decimal.MinValue;

            try
            {

                const byte ZEROB = 0x00;
                const byte FOURB = 0x04;
                const byte FIFTEENB = 0x0f;
                const byte SIXTYTHREEB = 0x3f;
                const byte ONETWENTYEIGHTB = 0x80;
                const char ZEROC = '0';
                const char PERIODC = '.';

                string decimalDelimiter = "" + PERIODC; // TODO: get locale decimial delimiter

                var retStr = "";
                byte sign;
                byte nibble;
                bool leadingZero = true;
                int nibblesLen = bCDDataLen * 2;
                int nibblesIter = 0;

                byte currByte = ZEROB;


                // Firstly start by reading the first byte, which contains the sign and decimal size.

                currByte = reader.ReadByte();

                if ((currByte & ONETWENTYEIGHTB) > 0) // Positive
                {
                    sign = ZEROB;
                }
                else // Negative
                {
                    sign = FIFTEENB;
                    retStr += "-";
                }
                int decLen = currByte & SIXTYTHREEB; // The encoded size of the decimal component of this BCD
                if (checkBCDDecLen && (decLen != bCDDecLen)) // Check that the encoded size matches what we expect
                {
                    //return decimal.MinValue;
                    throw new Exception("BCD decimal length does not match expected value.");
                }


                // Now we get the chars before the decimal.

                for (nibblesIter = 2; nibblesIter < (nibblesLen - decLen); nibblesIter++)
                {
                    if ((nibblesIter % 2) > 0) // Odd
                    {
                        nibble = (byte)(currByte & FIFTEENB);
                    }
                    else
                    {
                        currByte = reader.ReadByte();
                        nibble = (byte)((currByte >> FOURB) & FIFTEENB);
                    }

                    int nibbleSigned = (nibble ^ sign);

                    if (leadingZero && (nibbleSigned > 0)) // We've found a nibble value more than zero, so turn off the leading zero flag.
                    {
                        leadingZero = false;
                    }
                    if (!leadingZero)
                    {
                        char nibbleChar = (char)(nibbleSigned + ZEROC);

                        retStr += nibbleChar;
                    }
                }

                // Did we have a leading zero? (I.e. no leading other chars?)

                if (leadingZero)
                {
                    retStr += ZEROC;
                }
                retStr += decimalDelimiter;

                // Now we get the chars after the decimal.

                for (; nibblesIter < nibblesLen; nibblesIter++)
                {
                    if ((nibblesIter % 2) > 0) // Odd
                    {
                        nibble = (byte)(currByte & FIFTEENB);
                    }
                    else
                    {
                        currByte = reader.ReadByte();
                        nibble = (byte)((currByte >> FOURB) & FIFTEENB);
                    }

                    int nibbleSigned = (nibble ^ sign);
                    char nibbleChar = (char)(nibbleSigned + ZEROC);

                    retStr += nibbleChar;
                }

                var parsed = decimal.TryParse(retStr, out ret);

                if (!parsed)
                {
                    throw new Exception("Could not parse BCD: '" + retStr + "'");
                }

            }
            catch //(Exception ex)
            {
                if(returnMinValueInsteadOfThrow)
                {
                    ret = decimal.MinValue;
                }
                else
                {
                    throw;
                }
            }

            return ret;
        }
    }

    public class ParadoxRecord
    {
        internal readonly ParadoxFile.DataBlock block;
        private readonly int recIndex;

        internal ParadoxRecord(ParadoxFile.DataBlock block, int recIndex)
        {
            this.block = block;
            this.recIndex = recIndex;
        }

        private object[] data;

        public const int BCDDataSize = 17;
        public const int GraphicHsize = 17;
        public const int OtherBlobHsize = 9;

        public object[] DataValues
        {
            get
            {
                if (this.data == null)
                {
                    var buff = new MemoryStream(this.block.data);
                    buff.Position = this.block.file.RecordSize * this.recIndex;
                    using (var r = new BinaryReader(buff, Encoding.Default))
                    {
                        this.data = new object[this.block.file.FieldCount];
                        for (int colIndex = 0; colIndex < this.data.Length; colIndex++)
                        {
                            var fInfo = this.block.file.FieldTypes[colIndex];
                            var dataSize = fInfo.fType == ParadoxFieldTypes.BCD ? BCDDataSize : fInfo.fSize;
                            var bCDDecLen = fInfo.fType == ParadoxFieldTypes.BCD ? fInfo.fSize : 0;
                            var blobHsize = fInfo.fType == ParadoxFieldTypes.Graphic ? GraphicHsize : OtherBlobHsize;
                            var empty = true;
                            for (var i = 0; i < dataSize; i++)
                            {
                                if (this.block.data[buff.Position + i] != 0)
                                {
                                    empty = false;
                                    break;
                                }
                            }
                            if (empty)
                            {
                                this.data[colIndex] = DBNull.Value;
                                buff.Position += dataSize;
                                continue;
                            }
                            object val;
                            switch (fInfo.fType)
                            {
                                case ParadoxFieldTypes.Alpha:
                                    val = this.block.file.GetString(this.block.data, (int)buff.Position, dataSize);
                                    buff.Position += dataSize;
                                    break;
                                case ParadoxFieldTypes.Short:
                                    ConvertBytes((int)buff.Position, dataSize);
                                    val = r.ReadInt16();
                                    break;
                                case ParadoxFieldTypes.Long:
                                case ParadoxFieldTypes.AutoInc:
                                    ConvertBytes((int)buff.Position, dataSize);
                                    val = r.ReadInt32();
                                    break;
                                case ParadoxFieldTypes.Currency:
                                case ParadoxFieldTypes.Number:
                                    ConvertBytesNum((int)buff.Position, dataSize);
                                    var dbl = r.ReadDouble();
                                    val = (double.IsNaN(dbl)) ? (object)DBNull.Value : dbl;
                                    break;
                                case ParadoxFieldTypes.BCD:
                                    var decBCD = r.ReadBCD(dataSize, bCDDecLen);
                                    var dblBCD = decBCD > decimal.MinValue ? (double)decBCD : double.NaN;
                                    val = (double.IsNaN(dblBCD)) ? (object)DBNull.Value : dblBCD;
                                    break;
                                case ParadoxFieldTypes.Date:
                                    ConvertBytes((int)buff.Position, dataSize);
                                    var days = r.ReadInt32();
                                    val = new DateTime(1, 1, 1).AddDays(days > 0 ? days - 1 : 0);
                                    break;
                                case ParadoxFieldTypes.Timestamp:
                                    ConvertBytes((int)buff.Position, dataSize);
                                    var msDbl = r.ReadDouble();
                                    val = new DateTime(1, 1, 1).AddMilliseconds(msDbl >= 0 ? msDbl : 0).AddDays(msDbl >= 86400000 ? -1 : 0);
                                    break;
                                case ParadoxFieldTypes.Time:
                                    ConvertBytes((int)buff.Position, dataSize);
                                    var msInt = r.ReadInt32();
                                    val = TimeSpan.FromMilliseconds(msInt >= 0 ? msInt : 0);
                                    break;
                                case ParadoxFieldTypes.Logical:
                                    // False is stored as 128, and True looks like 129.
                                    val = (this.block.data[(int)buff.Position] - 128) > 0;
                                    buff.Position += dataSize;
                                    break;
                                case ParadoxFieldTypes.BLOb:
                                case ParadoxFieldTypes.OLE:
                                case ParadoxFieldTypes.Graphic:
                                case ParadoxFieldTypes.MemoBLOb:
                                case ParadoxFieldTypes.FmtMemoBLOb:
                                    var blobInfo = r.ReadBytes(dataSize);
                                    val = this.block.file.ReadBlob(blobInfo, dataSize, blobHsize);
                                    var isMemo = (fInfo.fType == ParadoxFieldTypes.MemoBLOb || fInfo.fType == ParadoxFieldTypes.FmtMemoBLOb);
                                    if (val != null && val is byte[])
                                    {
                                        if (isMemo)
                                        {
                                            val = Encoding.Default.GetString((byte[])val);
                                        }
                                        else
                                        {
                                            //val = Convert.ToBase64String((byte[])val);
                                        }
                                    }
                                    else
                                    {
                                        if (isMemo)
                                        {
                                            val = (string)null;
                                        }
                                        else
                                        {
                                            val = (byte[])null;
                                        }
                                    }
                                    break;
                                case ParadoxFieldTypes.Bytes:
                                    //val = r.ReadBytesIntoBase64String(dataSize); // Do we want bytes as bytes or base64 string?
                                    val = r.ReadBytes(dataSize); // Do we want bytes as bytes or base64 string?
                                    break;
                                default:
                                    val = null; // not supported
                                    buff.Position += dataSize;
                                    break;
                            }
                            this.data[colIndex] = val;
                        }
                    }
                }
                return this.data;
            }
        }

        private void ConvertBytes(int start, int length)
        {
            this.block.data[start] = (byte)(this.block.data[start] ^ 0x80);
            Array.Reverse(this.block.data, start, length);
        }

        private void ConvertBytesNum(int start, int length) /* amk */
        {
            if (((byte)(this.block.data[start]) & 0x80) != 0)
                this.block.data[start] = (byte)(this.block.data[start] & 0x7F);
            else if (this.block.data[start + 0] == 0 &&
                        this.block.data[start + 1] == 0 &&
                        this.block.data[start + 2] == 0 &&
                        this.block.data[start + 3] == 0 &&
                        this.block.data[start + 4] == 0 &&
                        this.block.data[start + 5] == 0 &&
                        this.block.data[start + 6] == 0 &&
                        this.block.data[start + 7] == 0) /* sorry, did not check lenght */
                ;
            else for (int i = 0; i < 8; i++)
                    this.block.data[start + i] = (byte)(~(this.block.data[start + i]));

            Array.Reverse(this.block.data, start, length);
        }

    }

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

    }

}
