using System;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Serializes Paradox field values to raw bytes and compares
    /// serialized key byte arrays field by field.
    /// Matches the binary encoding used in .DB, .PX, and .Xnn files.
    /// </summary>
    internal static class KeySerializer
    {
        // ----------------------------------------------------------------
        // Serialize
        // ----------------------------------------------------------------

        public static byte[] Serialize(object[] fieldValues, ParadoxFile.FieldInfo[] fields)
        {
            using (var ms = new MemoryStream())
            using (var w  = new BinaryWriter(ms))
            {
                for (int i = 0; i < fields.Length; i++)
                {
                    object value = (fieldValues != null && i < fieldValues.Length)
                        ? fieldValues[i] : null;
                    WriteField(w, value, fields[i]);
                }
                return ms.ToArray();
            }
        }

        private static void WriteField(BinaryWriter w, object value, ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                {
                    string str   = value as string ?? "";
                    byte[] bytes = Encoding.Default.GetBytes(str);
                    Array.Resize(ref bytes, field.fSize);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Short:
                {
                    short v = value == null ? (short)0
                            : value is short s ? s
                            : Convert.ToInt16(value);
                    ushort encoded = (ushort)(v ^ unchecked((short)0x8000));
                    w.Write((byte)(encoded >> 8));
                    w.Write((byte)(encoded & 0xFF));
                    break;
                }

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                {
                    int  v       = value == null ? 0
                                 : value is int i ? i
                                 : Convert.ToInt32(value);
                    uint encoded = (uint)(v ^ unchecked((int)0x80000000));
                    WriteBigEndianUInt32(w, encoded);
                    break;
                }

                case ParadoxFieldTypes.Date:
                {
                    int v = value == null          ? 0
                          : value is int i         ? i
                          : value is DateTime dt   ? DateTimeToParadoxDate(dt)
                          : Convert.ToInt32(value);
                    WriteBigEndianUInt32(w, (uint)(v ^ unchecked((int)0x80000000)));
                    break;
                }

                case ParadoxFieldTypes.Time:
                {
                    int v = value == null          ? 0
                          : value is int i         ? i
                          : value is TimeSpan ts   ? (int)ts.TotalMilliseconds
                          : Convert.ToInt32(value);
                    WriteBigEndianUInt32(w, (uint)(v ^ unchecked((int)0x80000000)));
                    break;
                }

                case ParadoxFieldTypes.Timestamp:
                {
                    double v = value == null        ? 0.0
                             : value is double d    ? d
                             : value is DateTime dt ? DateTimeToParadoxTimestamp(dt)
                             : Convert.ToDouble(value);
                    byte[] bytes = BitConverter.GetBytes(v);
                    BinaryReaderWriterPdoxExtensions.ConvertBytesForDouble(bytes, 8, false);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                {
                    double v = value == null     ? 0.0
                             : value is double d ? d
                             : Convert.ToDouble(value);
                    byte[] bytes = BitConverter.GetBytes(v);
                    BinaryReaderWriterPdoxExtensions.ConvertBytesForDouble(bytes, 8, false);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Logical:
                {
                    bool b = value != null && Convert.ToBoolean(value);
                    w.Write(b ? (byte)0x81 : (byte)0x80);
                    break;
                }

                case ParadoxFieldTypes.BCD:
                {
                    decimal v = value == null          ? 0m
                              : value is decimal dec   ? dec
                              : Convert.ToDecimal(value);
                    w.WritePdoxBCD(v, bCDDecLen: field.fSize, bCDDataLen: 17);
                    break;
                }

                case ParadoxFieldTypes.Bytes:
                {
                    byte[] bytes = value as byte[] ?? new byte[field.fSize];
                    if (bytes.Length != field.fSize)
                        Array.Resize(ref bytes, field.fSize);
                    w.Write(bytes);
                    break;
                }

                // BLOB types: stored as an offset/length reference, not inline data
                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                {
                    byte[] blobRef = value as byte[] ?? new byte[field.fSize];
                    if (blobRef.Length != field.fSize)
                        Array.Resize(ref blobRef, field.fSize);
                    w.Write(blobRef);
                    break;
                }

                default:
                    w.Write(new byte[field.fSize]);
                    break;
            }
        }

        // ----------------------------------------------------------------
        // Compare
        // ----------------------------------------------------------------

        public static int Compare(byte[] a, byte[] b, ParadoxFile.FieldInfo[] fields)
        {
            int offset = 0;
            foreach (var field in fields)
            {
                int result = CompareField(a, b, offset, field);
                if (result != 0) return result;
                offset += field.fSize;
            }
            return 0;
        }

        private static int CompareField(byte[] a, byte[] b, int offset, ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                {
                    string sa = Encoding.Default
                        .GetString(a, offset, field.fSize).TrimEnd('\0');
                    string sb = Encoding.Default
                        .GetString(b, offset, field.fSize).TrimEnd('\0');
                    return string.Compare(sa, sb, StringComparison.Ordinal);
                }

                case ParadoxFieldTypes.Short:
                {
                    ushort va = (ushort)((a[offset] << 8) | a[offset + 1]);
                    ushort vb = (ushort)((b[offset] << 8) | b[offset + 1]);
                    return va.CompareTo(vb);
                }

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Time:
                {
                    uint va = ReadBigEndianUInt32(a, offset);
                    uint vb = ReadBigEndianUInt32(b, offset);
                    return va.CompareTo(vb);
                }

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                case ParadoxFieldTypes.Timestamp:
                {
                    for (int i = 0; i < field.fSize; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }

                case ParadoxFieldTypes.Logical:
                    return a[offset].CompareTo(b[offset]);

                case ParadoxFieldTypes.BCD:
                {
                    for (int i = 0; i < 17; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }

                default:
                {
                    for (int i = 0; i < field.fSize; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }
            }
        }

        // ----------------------------------------------------------------
        // Date / Timestamp helpers
        // ----------------------------------------------------------------

        public static int DateTimeToParadoxDate(DateTime dt)
            => (int)(dt.Date - new DateTime(1, 1, 1)).TotalDays + 1;

        public static double DateTimeToParadoxTimestamp(DateTime dt)
        {
            double days = (dt.Date - new DateTime(1, 1, 1)).TotalDays + 1;
            double ms   = dt.TimeOfDay.TotalMilliseconds;
            return days * 86400000.0 + ms;
        }

        // ----------------------------------------------------------------
        // Endian helpers
        // ----------------------------------------------------------------

        private static void WriteBigEndianUInt32(BinaryWriter w, uint value)
        {
            w.Write((byte)((value >> 24) & 0xFF));
            w.Write((byte)((value >> 16) & 0xFF));
            w.Write((byte)((value >>  8) & 0xFF));
            w.Write((byte)( value        & 0xFF));
        }

        private static uint ReadBigEndianUInt32(byte[] data, int offset)
            => (uint)((data[offset]     << 24)
                    | (data[offset + 1] << 16)
                    | (data[offset + 2] <<  8)
                    |  data[offset + 3]);
    }
}
