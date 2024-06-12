using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using ParadoxReader;

namespace ParadoxTest
{
    internal class Program
    {


        static void Main(string[] args)
        {

            var dbPath = ParadoxTest.Configuration.GetParadoxDataFolderPath("Test");

            Console.WriteLine("Test 1: sequential read first 10 records from start");
            Console.WriteLine("==========================================================");
            using (var table = new ParadoxTable(dbPath, "testtab"))
            {
                var recIndex = 1;
                foreach (var rec in table.Enumerate())
                {
                    Console.WriteLine("Record #{0}", recIndex++);
                    for (int i = 0; i < table.FieldCount; i++)
                    {
                        var fieldName = table.FieldNames[i] ?? string.Empty;
                        var dataValue = rec.DataValues[i];
                        var dataValueToStr = dataValue?.ToString() ?? string.Empty;
                        if(dataValue != null && dataValue is byte[])
                        {
                            dataValueToStr = Convert.ToBase64String((byte[])dataValue);
                        }
                        Console.WriteLine("    {0} = {1}", fieldName, dataValueToStr);
                    }
                    if (recIndex > 10) break;
                }
                Console.WriteLine("-- press any key to continue --");
                Console.ReadKey();
                //Console.Clear();

                Console.WriteLine("Test 2: read 10 records by index (key range: 3 -> 4)");
                Console.WriteLine("==========================================================");

                using (var index = table.PrimaryKeyIndex)
                {
                    if (index != null)
                    {
                        var condition =
                            new ParadoxCondition.LogicalAnd(
                                new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, 3, 0, 0),
                                new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, 4, 0, 0));
                        var qry = index.Enumerate(condition);
                        var rdr = new ParadoxDataReader(table, qry);
                        recIndex = 1;
                        while (rdr.Read())
                        {
                            Console.WriteLine("Record #{0}", recIndex++);
                            for (int i = 0; i < rdr.FieldCount; i++)
                            {
                                Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                            }
                        }
                    }
                }
            }
            Console.WriteLine("-- press any key to continue --");
            Console.ReadKey();

        }
    }
}
