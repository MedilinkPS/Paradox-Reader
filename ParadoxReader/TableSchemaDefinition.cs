using System;
using System.Collections.Generic;
using System.Linq;

namespace ParadoxReader
{
    /// <summary>
    /// Describes a single field of a table schema being created or edited via
    /// <see cref="TableStructureDefinition"/>. This is a UI/edit-time model,
    /// distinct from the internal <see cref="ParadoxFile.FieldInfo"/> used by
    /// the low-level reader/writer, so it can be freely mutated (renamed,
    /// reordered, retyped) before being committed to disk.
    /// </summary>
    public class TableFieldDefinition
    {
        public string Name { get; set; }
        public ParadoxFieldTypes Type { get; set; }
        public byte Size { get; set; }

        /// <summary>
        /// True if this field is (part of) the primary key. Per Paradox
        /// convention (and this app's UI), primary-key fields are always
        /// the leading N fields of the table in declaration order - there
        /// is no independent ordering of key fields.
        /// </summary>
        public bool IsPrimaryKey { get; set; }

        public TableFieldDefinition()
        {
        }

        public TableFieldDefinition(string name, ParadoxFieldTypes type, byte size, bool isPrimaryKey)
        {
            Name = name;
            Type = type;
            Size = size;
            IsPrimaryKey = isPrimaryKey;
        }

        public TableFieldDefinition Clone()
        {
            return new TableFieldDefinition(Name, Type, Size, IsPrimaryKey);
        }
    }

    /// <summary>
    /// Describes one secondary index (a single logical .Xnn/.Xgn index, plus
    /// its .Ynn/.Ygn "maintained field" companion) to be created for a table,
    /// in terms of the (ordered) fields it indexes.
    /// </summary>
    public class TableIndexDefinition
    {
        /// <summary>
        /// Ordered list of field indices (into the owning
        /// <see cref="TableSchemaDefinition.Fields"/> list) that make up this
        /// index, in the order they should be indexed on.
        /// </summary>
        public List<int> FieldIndices { get; set; } = new List<int>();

        public TableIndexDefinition()
        {
        }

        public TableIndexDefinition(IEnumerable<int> fieldIndices)
        {
            FieldIndices = new List<int>(fieldIndices);
        }
    }

    /// <summary>
    /// Editable, in-memory description of a Paradox table's structure: its
    /// fields (name/type/size/primary-key flag) and secondary indexes. Used
    /// by the Table Structure UI (create/modify modes) and by
    /// <see cref="TableCreator"/>/<see cref="TableRebuilder"/> to build or
    /// regenerate the on-disk files.
    /// </summary>
    public class TableSchemaDefinition
    {
        public string TableName { get; set; }

        /// <summary>
        /// All fields in physical order. Primary-key fields must be the
        /// leading entries of this list (see <see cref="TableFieldDefinition.IsPrimaryKey"/>).
        /// </summary>
        public List<TableFieldDefinition> Fields { get; set; } = new List<TableFieldDefinition>();

        public List<TableIndexDefinition> Indexes { get; set; } = new List<TableIndexDefinition>();

        public int PrimaryKeyFieldCount => Fields.Count(f => f.IsPrimaryKey);

        /// <summary>
        /// Re-sorts <see cref="Fields"/> so all primary-key fields come first,
        /// preserving relative order within each group. Should be called
        /// whenever a field's <see cref="TableFieldDefinition.IsPrimaryKey"/>
        /// flag changes.
        /// </summary>
        public void ReorderPrimaryKeyFieldsFirst()
        {
            var pk = Fields.Where(f => f.IsPrimaryKey).ToList();
            var rest = Fields.Where(f => !f.IsPrimaryKey).ToList();
            Fields = pk.Concat(rest).ToList();
        }

        /// <summary>
        /// Builds a <see cref="TableSchemaDefinition"/> snapshot of an
        /// already-open table's current structure, suitable as the starting
        /// point for "Modify Structure" editing.
        /// </summary>
        public static TableSchemaDefinition FromTable(ParadoxTableFile table)
        {
            var schema = new TableSchemaDefinition
            {
                TableName = table.TableName
            };

            int primaryKeyCount = table.primaryKeyFields;
            for (int i = 0; i < table.FieldNames.Length; i++)
            {
                var field = table.FieldTypes[i];
                schema.Fields.Add(new TableFieldDefinition(
                    table.FieldNames[i], field.fType, field.fSize, i < primaryKeyCount));
            }

            foreach (var index in table.SecondaryIndexes)
            {
                schema.Indexes.Add(new TableIndexDefinition(index.FieldIndices));
            }

            return schema;
        }
    }
}
