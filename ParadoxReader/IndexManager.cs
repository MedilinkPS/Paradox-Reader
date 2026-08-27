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

        // ----------------------------------------------------------------
        // Secondary index discovery
        // ----------------------------------------------------------------

        private void DiscoverAndOpenSecondaryIndexes(string dbFilePath)
        {
            var discovered = SecondaryIndexDiscovery.Discover(dbFilePath, allFields);
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