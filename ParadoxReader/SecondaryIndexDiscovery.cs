using System;
using System.Collections.Generic;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Scans the directory of a .DB file for all secondary index files
    /// (.Xnn, .Xgn) and reads their headers to determine which fields
    /// they index.
    /// </summary>
    internal static class SecondaryIndexDiscovery
    {
        public static List<SecondaryIndexInfo> Discover(
            string dbFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount)
        {
            var    result = new List<SecondaryIndexInfo>();
            string dir    = Path.GetDirectoryName(dbFilePath) ?? ".";
            string name   = Path.GetFileNameWithoutExtension(dbFilePath);

            // Non-incremental secondary indexes: .X00 - .X99
            TryScanPattern(dir, name, "X",  padWidth: 2, from: 0, to: 99,  allFields, primaryKeyFieldCount, result);

            // Incremental secondary indexes: .XG0 - .XG9
            TryScanPattern(dir, name, "XG", padWidth: 1, from: 0, to: 9,   allFields, primaryKeyFieldCount, result);

            // Maintained-field companions to non-incremental indexes: .Y00 - .Y99
            TryScanPattern(dir, name, "Y",  padWidth: 2, from: 0, to: 99,  allFields, primaryKeyFieldCount, result);

            // Maintained-field companions to incremental indexes: .YG0 - .YG9
            TryScanPattern(dir, name, "YG", padWidth: 1, from: 0, to: 9,   allFields, primaryKeyFieldCount, result);

            return result;
        }

        private static void TryScanPattern(
            string dir, string baseName, string prefix,
            int padWidth, int from, int to,
            ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount, List<SecondaryIndexInfo> result)
        {
            for (int i = from; i <= to; i++)
            {
                string ext  = $".{prefix}{i.ToString().PadLeft(padWidth, '0')}";
                string path = Path.Combine(dir, baseName + ext);
                if (!File.Exists(path)) continue;

                try
                {
                    var info = ReadIndexHeader(path, allFields, primaryKeyFieldCount);
                    if (info != null) result.Add(info);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] Skipping {path}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Opens an index file and reads its header to determine which
        /// parent-table fields it indexes. Uses indexFieldNumber from the
        /// header (1-based field position in the parent table) for an exact
        /// mapping, falling back to type+size matching only when it is zero.
        /// </summary>
        private static SecondaryIndexInfo ReadIndexHeader(
            string indexFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount)
        {
            using (var f = new ParadoxFile(indexFilePath))
            {
                if (f.FieldCount <= 0 || f.FieldTypes == null)
                    return null;

                var indexedFields = new List<ParadoxFile.FieldInfo>();
                var fieldIndices  = new List<int>();

                int startField = f.indexFieldNumber - 1; // convert 1-based → 0-based

                if (startField >= 0 && startField < allFields.Length)
                {
                    // Primary path: use the explicit field number from the header.
                    // The secondary index key is composed of the indexed field(s)
                    // followed by the parent table's primary key field(s), which
                    // are appended for uniqueness. f.FieldCount reflects this total,
                    // so the indexed portion is FieldCount - primaryKeyFieldCount
                    // (falling back to the full count when that would be non-positive).
                    int indexedFieldCount = f.FieldCount - primaryKeyFieldCount;
                    if (indexedFieldCount <= 0) indexedFieldCount = f.FieldCount;

                    for (int i = 0; i < indexedFieldCount && (startField + i) < allFields.Length; i++)
                    {
                        indexedFields.Add(allFields[startField + i]);
                        fieldIndices.Add(startField + i);
                    }

                    // Append the primary key fields (positions 0..primaryKeyFieldCount-1)
                    // so the composed key matches what Paradox stores on disk, avoiding
                    // duplicate-key collisions and preventing later fields from being
                    // misinterpreted as extra indexed columns.
                    for (int i = 0; i < primaryKeyFieldCount && i < allFields.Length; i++)
                    {
                        if (fieldIndices.Contains(i)) continue;
                        indexedFields.Add(allFields[i]);
                        fieldIndices.Add(i);
                    }
                }
                else
                {
                    // Fallback: match by type + size (ambiguous when fields share both).
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] '{indexFilePath}': indexFieldNumber={f.indexFieldNumber} " +
                        $"is out of range (allFields.Length={allFields.Length}), falling back to type+size match.");

                    foreach (var idxField in f.FieldTypes)
                    {
                        for (int j = 0; j < allFields.Length; j++)
                        {
                            if (allFields[j].fType == idxField.fType &&
                                allFields[j].fSize == idxField.fSize)
                            {
                                indexedFields.Add(allFields[j]);
                                fieldIndices.Add(j);
                                break;
                            }
                        }
                    }
                }

                if (indexedFields.Count == 0)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] '{indexFilePath}': could not map any fields — skipping.");
                    return null;
                }

                System.Diagnostics.Debug.WriteLine(
                    $"[SecondaryIndexDiscovery] '{indexFilePath}': mapped {indexedFields.Count} field(s), " +
                    $"indices=[{string.Join(",", fieldIndices)}]");

                return new SecondaryIndexInfo
                {
                    FilePath      = indexFilePath,
                    IndexedFields = indexedFields.ToArray(),
                    FieldIndices  = fieldIndices.ToArray()
                };
            }
        }
    }
}