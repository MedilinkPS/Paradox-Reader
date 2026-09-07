namespace ParadoxDesktop
{
    partial class TableStructureForm
    {
        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        private void InitializeComponent()
        {
            this.summaryLabel = new System.Windows.Forms.Label();
            this.tableNameLabel = new System.Windows.Forms.Label();
            this.tableNameTextBox = new System.Windows.Forms.TextBox();
            this.fieldsListView = new System.Windows.Forms.ListView();
            this.colName = new System.Windows.Forms.ColumnHeader();
            this.colType = new System.Windows.Forms.ColumnHeader();
            this.colSize = new System.Windows.Forms.ColumnHeader();
            this.colPrimaryKey = new System.Windows.Forms.ColumnHeader();
            this.editorPanel = new System.Windows.Forms.Panel();
            this.fieldPrimaryKeyCheckBox = new System.Windows.Forms.CheckBox();
            this.fieldSizeNumericUpDown = new System.Windows.Forms.NumericUpDown();
            this.fieldSizeLabel = new System.Windows.Forms.Label();
            this.fieldTypeComboBox = new System.Windows.Forms.ComboBox();
            this.fieldTypeLabel = new System.Windows.Forms.Label();
            this.fieldNameTextBox = new System.Windows.Forms.TextBox();
            this.fieldNameLabel = new System.Windows.Forms.Label();
            this.addUpdateFieldButton = new System.Windows.Forms.Button();
            this.removeFieldButton = new System.Windows.Forms.Button();
            this.indexesLabel = new System.Windows.Forms.Label();
            this.indexesListBox = new System.Windows.Forms.ListBox();
            this.indexButtonsPanel = new System.Windows.Forms.Panel();
            this.removeIndexButton = new System.Windows.Forms.Button();
            this.addEditIndexButton = new System.Windows.Forms.Button();
            this.okButton = new System.Windows.Forms.Button();
            this.cancelButton = new System.Windows.Forms.Button();
            this.editorPanel.SuspendLayout();
            this.indexButtonsPanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.fieldSizeNumericUpDown)).BeginInit();
            this.SuspendLayout();
            //
            // summaryLabel
            //
            this.summaryLabel.AutoSize = true;
            this.summaryLabel.Location = new System.Drawing.Point(12, 9);
            this.summaryLabel.Name = "summaryLabel";
            this.summaryLabel.Size = new System.Drawing.Size(52, 13);
            this.summaryLabel.TabIndex = 0;
            this.summaryLabel.Text = "Summary";
            //
            // tableNameLabel
            //
            this.tableNameLabel.AutoSize = true;
            this.tableNameLabel.Location = new System.Drawing.Point(12, 32);
            this.tableNameLabel.Name = "tableNameLabel";
            this.tableNameLabel.Size = new System.Drawing.Size(66, 13);
            this.tableNameLabel.TabIndex = 1;
            this.tableNameLabel.Text = "Table Name:";
            //
            // tableNameTextBox
            //
            this.tableNameTextBox.Location = new System.Drawing.Point(90, 29);
            this.tableNameTextBox.Name = "tableNameTextBox";
            this.tableNameTextBox.Size = new System.Drawing.Size(200, 20);
            this.tableNameTextBox.TabIndex = 2;
            //
            // fieldsListView
            //
            this.fieldsListView.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.colName,
            this.colType,
            this.colSize,
            this.colPrimaryKey});
            this.fieldsListView.FullRowSelect = true;
            this.fieldsListView.GridLines = true;
            this.fieldsListView.HideSelection = false;
            this.fieldsListView.Location = new System.Drawing.Point(12, 56);
            this.fieldsListView.Name = "fieldsListView";
            this.fieldsListView.Size = new System.Drawing.Size(460, 140);
            this.fieldsListView.TabIndex = 3;
            this.fieldsListView.UseCompatibleStateImageBehavior = false;
            this.fieldsListView.View = System.Windows.Forms.View.Details;
            //
            // colName
            //
            this.colName.Text = "Field Name";
            this.colName.Width = 160;
            //
            // colType
            //
            this.colType.Text = "Type";
            this.colType.Width = 120;
            //
            // colSize
            //
            this.colSize.Text = "Size";
            this.colSize.Width = 60;
            //
            // colPrimaryKey
            //
            this.colPrimaryKey.Text = "Primary Key";
            this.colPrimaryKey.Width = 90;
            //
            // editorPanel
            //
            this.editorPanel.Controls.Add(this.fieldPrimaryKeyCheckBox);
            this.editorPanel.Controls.Add(this.fieldSizeNumericUpDown);
            this.editorPanel.Controls.Add(this.fieldSizeLabel);
            this.editorPanel.Controls.Add(this.fieldTypeComboBox);
            this.editorPanel.Controls.Add(this.fieldTypeLabel);
            this.editorPanel.Controls.Add(this.fieldNameTextBox);
            this.editorPanel.Controls.Add(this.fieldNameLabel);
            this.editorPanel.Controls.Add(this.addUpdateFieldButton);
            this.editorPanel.Controls.Add(this.removeFieldButton);
            this.editorPanel.Location = new System.Drawing.Point(12, 200);
            this.editorPanel.Name = "editorPanel";
            this.editorPanel.Size = new System.Drawing.Size(460, 42);
            this.editorPanel.TabIndex = 4;
            //
            // fieldPrimaryKeyCheckBox
            //
            this.fieldPrimaryKeyCheckBox.AutoSize = true;
            this.fieldPrimaryKeyCheckBox.Location = new System.Drawing.Point(300, 8);
            this.fieldPrimaryKeyCheckBox.Name = "fieldPrimaryKeyCheckBox";
            this.fieldPrimaryKeyCheckBox.Size = new System.Drawing.Size(51, 17);
            this.fieldPrimaryKeyCheckBox.TabIndex = 8;
            this.fieldPrimaryKeyCheckBox.Text = "PK";
            this.fieldPrimaryKeyCheckBox.UseVisualStyleBackColor = true;
            //
            // fieldSizeNumericUpDown
            //
            this.fieldSizeNumericUpDown.Location = new System.Drawing.Point(240, 6);
            this.fieldSizeNumericUpDown.Maximum = new decimal(new int[] { 255, 0, 0, 0 });
            this.fieldSizeNumericUpDown.Minimum = new decimal(new int[] { 1, 0, 0, 0 });
            this.fieldSizeNumericUpDown.Name = "fieldSizeNumericUpDown";
            this.fieldSizeNumericUpDown.Size = new System.Drawing.Size(50, 20);
            this.fieldSizeNumericUpDown.TabIndex = 7;
            this.fieldSizeNumericUpDown.Value = new decimal(new int[] { 1, 0, 0, 0 });
            //
            // fieldSizeLabel
            //
            this.fieldSizeLabel.AutoSize = true;
            this.fieldSizeLabel.Location = new System.Drawing.Point(210, 9);
            this.fieldSizeLabel.Name = "fieldSizeLabel";
            this.fieldSizeLabel.Size = new System.Drawing.Size(27, 13);
            this.fieldSizeLabel.TabIndex = 6;
            this.fieldSizeLabel.Text = "Size:";
            //
            // fieldTypeComboBox
            //
            this.fieldTypeComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.fieldTypeComboBox.FormattingEnabled = true;
            this.fieldTypeComboBox.Location = new System.Drawing.Point(140, 6);
            this.fieldTypeComboBox.Name = "fieldTypeComboBox";
            this.fieldTypeComboBox.Size = new System.Drawing.Size(90, 21);
            this.fieldTypeComboBox.TabIndex = 5;
            //
            // fieldTypeLabel
            //
            this.fieldTypeLabel.AutoSize = true;
            this.fieldTypeLabel.Location = new System.Drawing.Point(105, 9);
            this.fieldTypeLabel.Name = "fieldTypeLabel";
            this.fieldTypeLabel.Size = new System.Drawing.Size(34, 13);
            this.fieldTypeLabel.TabIndex = 4;
            this.fieldTypeLabel.Text = "Type:";
            //
            // fieldNameLabel
            //
            this.fieldNameLabel.AutoSize = true;
            this.fieldNameLabel.Location = new System.Drawing.Point(0, 9);
            this.fieldNameLabel.Name = "fieldNameLabel";
            this.fieldNameLabel.Size = new System.Drawing.Size(38, 13);
            this.fieldNameLabel.TabIndex = 0;
            this.fieldNameLabel.Text = "Name:";
            //
            // fieldNameTextBox
            //
            this.fieldNameTextBox.Location = new System.Drawing.Point(40, 6);
            this.fieldNameTextBox.Name = "fieldNameTextBox";
            this.fieldNameTextBox.Size = new System.Drawing.Size(60, 20);
            this.fieldNameTextBox.TabIndex = 1;
            //
            // addUpdateFieldButton
            //
            this.addUpdateFieldButton.Location = new System.Drawing.Point(300, 4);
            this.addUpdateFieldButton.Name = "addUpdateFieldButton";
            this.addUpdateFieldButton.Size = new System.Drawing.Size(75, 23);
            this.addUpdateFieldButton.TabIndex = 9;
            this.addUpdateFieldButton.Text = "Add Field";
            this.addUpdateFieldButton.UseVisualStyleBackColor = true;
            this.addUpdateFieldButton.Click += new System.EventHandler(this.addUpdateFieldButton_Click);
            //
            // removeFieldButton
            //
            this.removeFieldButton.Enabled = false;
            this.removeFieldButton.Location = new System.Drawing.Point(381, 4);
            this.removeFieldButton.Name = "removeFieldButton";
            this.removeFieldButton.Size = new System.Drawing.Size(75, 23);
            this.removeFieldButton.TabIndex = 10;
            this.removeFieldButton.Text = "Remove";
            this.removeFieldButton.UseVisualStyleBackColor = true;
            this.removeFieldButton.Click += new System.EventHandler(this.removeFieldButton_Click);
            //
            // indexesLabel
            //
            this.indexesLabel.AutoSize = true;
            this.indexesLabel.Location = new System.Drawing.Point(12, 250);
            this.indexesLabel.Name = "indexesLabel";
            this.indexesLabel.Size = new System.Drawing.Size(84, 13);
            this.indexesLabel.TabIndex = 5;
            this.indexesLabel.Text = "Secondary Indexes";
            //
            // indexesListBox
            //
            this.indexesListBox.FormattingEnabled = true;
            this.indexesListBox.Location = new System.Drawing.Point(12, 268);
            this.indexesListBox.Name = "indexesListBox";
            this.indexesListBox.Size = new System.Drawing.Size(367, 82);
            this.indexesListBox.TabIndex = 6;
            //
            // indexButtonsPanel
            //
            this.indexButtonsPanel.Controls.Add(this.removeIndexButton);
            this.indexButtonsPanel.Controls.Add(this.addEditIndexButton);
            this.indexButtonsPanel.Location = new System.Drawing.Point(385, 268);
            this.indexButtonsPanel.Name = "indexButtonsPanel";
            this.indexButtonsPanel.Size = new System.Drawing.Size(87, 82);
            this.indexButtonsPanel.TabIndex = 7;
            //
            // removeIndexButton
            //
            this.removeIndexButton.Location = new System.Drawing.Point(3, 33);
            this.removeIndexButton.Name = "removeIndexButton";
            this.removeIndexButton.Size = new System.Drawing.Size(80, 23);
            this.removeIndexButton.TabIndex = 1;
            this.removeIndexButton.Text = "Remove";
            this.removeIndexButton.UseVisualStyleBackColor = true;
            this.removeIndexButton.Click += new System.EventHandler(this.removeIndexButton_Click);
            //
            // addEditIndexButton
            //
            this.addEditIndexButton.Location = new System.Drawing.Point(3, 4);
            this.addEditIndexButton.Name = "addEditIndexButton";
            this.addEditIndexButton.Size = new System.Drawing.Size(80, 23);
            this.addEditIndexButton.TabIndex = 0;
            this.addEditIndexButton.Text = "Add/Edit...";
            this.addEditIndexButton.UseVisualStyleBackColor = true;
            this.addEditIndexButton.Click += new System.EventHandler(this.addEditIndexButton_Click);
            //
            // okButton
            //
            this.okButton.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.okButton.Location = new System.Drawing.Point(316, 381);
            this.okButton.Name = "okButton";
            this.okButton.Size = new System.Drawing.Size(75, 23);
            this.okButton.TabIndex = 8;
            this.okButton.Text = "OK";
            this.okButton.UseVisualStyleBackColor = true;
            this.okButton.Click += new System.EventHandler(this.okButton_Click);
            //
            // cancelButton
            //
            this.cancelButton.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.cancelButton.Location = new System.Drawing.Point(397, 381);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 9;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            //
            // TableStructureForm
            //
            this.AcceptButton = this.okButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.cancelButton;
            this.ClientSize = new System.Drawing.Size(484, 416);
            this.Controls.Add(this.cancelButton);
            this.Controls.Add(this.okButton);
            this.Controls.Add(this.indexButtonsPanel);
            this.Controls.Add(this.indexesListBox);
            this.Controls.Add(this.indexesLabel);
            this.Controls.Add(this.editorPanel);
            this.Controls.Add(this.fieldsListView);
            this.Controls.Add(this.tableNameTextBox);
            this.Controls.Add(this.tableNameLabel);
            this.Controls.Add(this.summaryLabel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TableStructureForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Table Structure";
            this.editorPanel.ResumeLayout(false);
            this.editorPanel.PerformLayout();
            this.indexButtonsPanel.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.fieldSizeNumericUpDown)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label summaryLabel;
        private System.Windows.Forms.Label tableNameLabel;
        private System.Windows.Forms.TextBox tableNameTextBox;
        private System.Windows.Forms.ListView fieldsListView;
        private System.Windows.Forms.ColumnHeader colName;
        private System.Windows.Forms.ColumnHeader colType;
        private System.Windows.Forms.ColumnHeader colSize;
        private System.Windows.Forms.ColumnHeader colPrimaryKey;
        private System.Windows.Forms.Panel editorPanel;
        private System.Windows.Forms.CheckBox fieldPrimaryKeyCheckBox;
        private System.Windows.Forms.NumericUpDown fieldSizeNumericUpDown;
        private System.Windows.Forms.Label fieldSizeLabel;
        private System.Windows.Forms.ComboBox fieldTypeComboBox;
        private System.Windows.Forms.Label fieldTypeLabel;
        private System.Windows.Forms.TextBox fieldNameTextBox;
        private System.Windows.Forms.Label fieldNameLabel;
        private System.Windows.Forms.Button addUpdateFieldButton;
        private System.Windows.Forms.Button removeFieldButton;
        private System.Windows.Forms.Label indexesLabel;
        private System.Windows.Forms.ListBox indexesListBox;
        private System.Windows.Forms.Panel indexButtonsPanel;
        private System.Windows.Forms.Button removeIndexButton;
        private System.Windows.Forms.Button addEditIndexButton;
        private System.Windows.Forms.Button okButton;
        private System.Windows.Forms.Button cancelButton;
    }
}
