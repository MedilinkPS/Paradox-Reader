using System;
using System.IO;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Read-only dialog (Table > Info Structure) showing a table's field
    /// schema (name/type/size/primary-key membership) and any discovered
    /// secondary index files. Editing structure is a possible future
    /// enhancement; for now this is purely informational.
    /// </summary>
    public partial class TableInfoForm : Form
    {
        public TableInfoForm(ParadoxTableFile table)
        {
            InitializeComponent();

            if (table == null) return;

            summaryLabel.Text = string.Format("{0}  —  {1} field(s), {2} record(s){3}",
                Path.GetFileName(table.FilePath),
                table.FieldCount,
                table.RecordCount,
                table.IndexOutOfDate ? "  [INDEX OUT OF DATE]" : string.Empty);

            int primaryKeyCount = table.primaryKeyFields;

            for (int i = 0; i < table.FieldNames.Length; i++)
            {
                var field = table.FieldTypes[i];
                bool isPrimaryKey = i < primaryKeyCount;

                var item = new ListViewItem(table.FieldNames[i]);
                item.SubItems.Add(field.fType.ToString());
                item.SubItems.Add(field.fSize.ToString());
                item.SubItems.Add(isPrimaryKey ? "Yes" : string.Empty);
                fieldsListView.Items.Add(item);
            }

            if (table.SecondaryIndexes.Count == 0)
            {
                indexesListBox.Items.Add("(none)");
            }
            else
            {
                // .Xnn/.Xgn and their .Ynn/.Ygn "maintained field" companions
                // describe the same logical index (same indexed fields), so
                // group them together under one entry keyed by the index's
                // name - which for a Paradox secondary index is simply the
                // name of the (first) field it's built on - rather than
                // listing each file separately.
                var indexNames = new System.Collections.Generic.List<string>();
                var fieldsByIndexName = new System.Collections.Generic.Dictionary<string, string>();
                var extensionsByIndexName = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();

                foreach (var index in table.SecondaryIndexes)
                {
                    string extension = Path.GetExtension(index.FilePath);
                    var fieldNames = new string[index.FieldIndices.Length];
                    for (int i = 0; i < index.FieldIndices.Length; i++)
                    {
                        int fieldIndex = index.FieldIndices[i];
                        fieldNames[i] = fieldIndex >= 0 && fieldIndex < table.FieldNames.Length
                            ? table.FieldNames[fieldIndex]
                            : "?";
                    }

                    // Paradox names a secondary index after the first field
                    // it indexes (see the "Secondary Index Info" dialog in
                    // the original Paradox for Windows tool).
                    string indexName = fieldNames.Length > 0 ? fieldNames[0] : "?";

                    System.Collections.Generic.List<string> extensions;
                    if (!extensionsByIndexName.TryGetValue(indexName, out extensions))
                    {
                        extensions = new System.Collections.Generic.List<string>();
                        extensionsByIndexName[indexName] = extensions;
                        fieldsByIndexName[indexName] = string.Join(", ", fieldNames);
                        indexNames.Add(indexName);
                    }

                    extensions.Add(extension);
                }

                foreach (var indexName in indexNames)
                {
                    indexesListBox.Items.Add(string.Format("{0}  \u2014  fields: {1}; extensions: {2}",
                        indexName, fieldsByIndexName[indexName], string.Join(", ", extensionsByIndexName[indexName].ToArray())));
                }
            }
        }
    }
}
