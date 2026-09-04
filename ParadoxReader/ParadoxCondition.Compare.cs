namespace ParadoxReader
{
    public partial class ParadoxCondition
    {
        public class Compare : ParadoxCondition
        {
            public ParadoxCompareOperator Operator { get; private set; }
            public object Value { get; private set; }

            public int DataFieldIndex { get; private set; }
            public int IndexFieldIndex { get; private set; }

            public override bool IsDataOk(ParadoxReader.ParadoxRecord dataRec)
            {
                var val = dataRec.DataValues[this.DataFieldIndex];
                var comp = System.Collections.Comparer.Default.Compare(val, this.Value);
                switch (Operator)
                {
                    case ParadoxCompareOperator.Equal:
                        return comp == 0;
                    case ParadoxCompareOperator.NotEqual:
                        return comp != 0;
                    case ParadoxCompareOperator.Greater:
                        return comp > 0;
                    case ParadoxCompareOperator.GreaterOrEqual:
                        return comp >= 0;
                    case ParadoxCompareOperator.Less:
                        return comp < 0;
                    case ParadoxCompareOperator.LessOrEqual:
                        return comp <= 0;
                    default:
                        throw new System.NotSupportedException();
                }
            }

            public override bool IsIndexPossible(ParadoxReader.ParadoxRecord indexRec, ParadoxReader.ParadoxRecord nextRec)
            {
                // indexRec/nextRec here represent a synthesized record built directly
                // from an index leaf entry's key data (see SecondaryIndexFile.Enumerate
                // and ParadoxPrimaryKey.Enumerate). For secondary indexes, the composed
                // key layout differs from the parent table's field layout (indexed
                // field(s) followed by appended primary-key fields), so IndexFieldIndex
                // - not DataFieldIndex - must be used to look up the value in the key's
                // own coordinate system. For the primary index, IndexFieldIndex is
                // conventionally the same as DataFieldIndex.
                var val1 = indexRec.DataValues[this.IndexFieldIndex];
                var comp1 = System.Collections.Comparer.Default.Compare(val1, this.Value);
                int comp2;
                if (nextRec != null)
                {
                    var val2 = nextRec.DataValues[this.IndexFieldIndex];
                    comp2 = System.Collections.Comparer.Default.Compare(val2, this.Value);
                }
                else
                {
                    comp2 = 1; // last index range ends in infinite
                }
                switch (Operator)
                {
                    case ParadoxCompareOperator.Equal:
                        return comp1 <= 0 && comp2 >= 0;
                    case ParadoxCompareOperator.NotEqual:
                        return comp1 > 0 || comp2 < 0;
                    case ParadoxCompareOperator.Greater:
                        return comp2 > 0;
                    case ParadoxCompareOperator.GreaterOrEqual:
                        return comp2 >= 0;
                    case ParadoxCompareOperator.Less:
                        return comp1 < 0;
                    case ParadoxCompareOperator.LessOrEqual:
                        return comp1 <= 0;
                    default:
                        throw new System.NotSupportedException();
                }
            }

            public Compare(ParadoxCompareOperator op, object value, int dataFieldIndex, int indexFieldIndex)
            {
                Operator = op;
                Value = value;
                DataFieldIndex = dataFieldIndex;
                IndexFieldIndex = indexFieldIndex;
            }
        }
    }
}
