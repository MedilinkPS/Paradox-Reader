using System;
using System.Collections.Generic;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Coordinates updates across all index files (.PX, .Xnn, .Xgn)
    /// associated with a .DB table. Acts as the single point of contact
    /// for all index maintenance during insert, update, and delete operations.
    /// </summary>
    internal class IndexManager : IDisposable
    {
        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly PrimaryIndex         primaryIndex;
        private readonly List<SecondaryIndex> secondaryIndexes = new List<SecondaryIndex>();
        private readonly ParadoxFile.FieldInfo[]          allFields;
        private readonly int                  primaryKeyFieldCount;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public IndexManager(string dbFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount)
        {
            this.allFields            = allFields;
            this.primaryKeyFieldCount = primaryKeyFieldCount;

            // Primary index (.PX)
            if (primaryKeyFieldCount > 0)
            {
                string pxPath = Path.ChangeExtension(dbFilePath, ".PX");
                if (File.Exists(pxPath))
                {
                    var keyFields = GetFieldRange(0, primaryKeyFieldCount);
                    primaryIndex  = new PrimaryIndex(pxPath, keyFields);
                }
            }

            // Secondary indexes (.Xnn, .Xgn)
            DiscoverAndOpenSecondaryIndexes(dbFilePath);
        }

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        public void OnInsert(object[] fieldValues, ushort blockNumber, ushort recordIndex)
        {
            primaryIndex?.OnInsert(
                GetKeyValues(fieldValues, 0, primaryKeyFieldCount),
                blockNumber, recordIndex);

            foreach (var idx in secondaryIndexes)
                idx.OnInsert(fieldValues, blockNumber, recordIndex);
        }

        public void OnUpdate(object[] oldValues, object[] newValues,
                             ushort blockNumber, ushort recordIndex)
        {
            primaryIndex?.OnUpdate(
                GetKeyValues(oldValues, 0, primaryKeyFieldCount),
                GetKeyValues(newValues, 0, primaryKeyFieldCount),
                blockNumber, recordIndex);

            foreach (var idx in secondaryIndexes)
                idx.OnUpdate(oldValues, newValues, blockNumber, recordIndex);
        }

        public void OnDelete(object[] fieldValues)
        {
            primaryIndex?.OnDelete(
                GetKeyValues(fieldValues, 0, primaryKeyFieldCount));

            foreach (var idx in secondaryIndexes)
                idx.OnDelete(fieldValues);
        }

        /// <summary>
        /// Mirrors the parent .DB file's changeCount1/changeCount2 bytes into every
        /// open index file, so BDE does not consider the indexes out of date after
        /// a write. Must be called after every insert/update/delete, once the .DB
        /// header's change count has been incremented.
        /// </summary>
        public void SyncChangeCount(byte changeCount1, byte changeCount2)
        {
            primaryIndex?.SyncChangeCount(changeCount1, changeCount2);

            foreach (var idx in secondaryIndexes)
                idx.SyncChangeCount(changeCount1, changeCount2);
        }

        /// <summary>
        /// Mirrors the parent .DB file's autoIncVal (offset 0x49) into every
        /// open index file. BDE/Pdxrbld considers an index out of date if its
        /// own autoIncVal doesn't match the table's after an AutoInc field is
        /// assigned.
        /// </summary>
        public void SyncAutoIncVal(int autoIncVal)
        {
            primaryIndex?.SyncAutoIncVal(autoIncVal);

            foreach (var idx in secondaryIndexes)
                idx.SyncAutoIncVal(autoIncVal);
        }

        /// <summary>
        /// Mirrors the parent .DB file's V4Hdr changeCount4 (offset 0x70) into
        /// every open index file. BDE/Pdxrbld compares this "table version"
        /// counter against each index's own copy to decide whether the index
        /// is out of date.
        /// </summary>
        public void SyncTableVersion(short changeCount4)
        {
            primaryIndex?.SyncTableVersion(changeCount4);

            foreach (var idx in secondaryIndexes)
                idx.SyncTableVersion(changeCount4);
        }

        /// <summary>
        /// Increments the single-byte write counter at offset 0x2C in every
        /// open index file. Discovered via multi-pass SQLRunner testing:
        /// this byte increments by 1 on every write to a valid .PX/.Xnn
        /// file, independently of the .DB file's own changeCount. Being
        /// tried as the true "index version" field.
        /// </summary>
        public void IncrementWriteCounter()
        {
            primaryIndex?.IncrementWriteCounter();

            foreach (var idx in secondaryIndexes)
                idx.IncrementWriteCounter();
        }

        // ----------------------------------------------------------------
        // Secondary index discovery
        // ----------------------------------------------------------------

        private void DiscoverAndOpenSecondaryIndexes(string dbFilePath)
        {
            var discovered = SecondaryIndexDiscovery.Discover(dbFilePath, allFields, primaryKeyFieldCount);
            foreach (var info in discovered)
            {
                try
                {
                    secondaryIndexes.Add(
                        new SecondaryIndex(info.FilePath, info.IndexedFields, info.FieldIndices));
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[IndexManager] Failed to open {info.FilePath}: {ex.Message}");
                }
            }
        }

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        private ParadoxFile.FieldInfo[] GetFieldRange(int start, int count)
        {
            var fields = new ParadoxFile.FieldInfo[count];
            Array.Copy(allFields, start, fields, 0, count);
            return fields;
        }

        private object[] GetKeyValues(object[] fieldValues, int start, int count)
        {
            if (count <= 0 || fieldValues == null) return new object[0];
            var keys = new object[count];
            Array.Copy(fieldValues, start, keys, 0,
                       Math.Min(count, fieldValues.Length - start));
            return keys;
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose()
        {
            primaryIndex?.Dispose();
            foreach (var idx in secondaryIndexes)
                idx.Dispose();
        }
    }
}