using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Forms;
using ArcGIS.Core.CIM;
using ArcGIS.Core.Data;
using ArcGIS.Core.Geometry;
using ArcGIS.Desktop.Catalog;
using ArcGIS.Desktop.Core;
using ArcGIS.Desktop.Editing;
using ArcGIS.Desktop.Extensions;
using ArcGIS.Desktop.Framework;
using ArcGIS.Desktop.Framework.Contracts;
using ArcGIS.Desktop.Framework.Dialogs;
using ArcGIS.Desktop.Framework.Threading.Tasks;
using ArcGIS.Desktop.Mapping;
using Microsoft.Win32;
using System.IO;
using System.Collections.ObjectModel;
using ArcGIS.Desktop.Core.Geoprocessing;
using ActiproSoftware.Windows.Extensions;
using System.Security.Cryptography.X509Certificates;
using System.Diagnostics;
using System.Threading;
using ActiproSoftware.Products.Ribbon;

namespace CSV2DBPro
{
    internal class CSV2Db : ArcGIS.Desktop.Framework.Contracts.Button
    {
        protected override void OnClick()
        {
            string csvFilePath = "";

            System.Windows.Forms.OpenFileDialog getCSV = new System.Windows.Forms.OpenFileDialog();
            getCSV.Filter = "CSV Files | *.txt";
            if (getCSV.ShowDialog() == DialogResult.OK)
            {
                csvFilePath = getCSV.FileName;
            }

            // OK, we have the selected csv file, so lets get to work

            FileStream fs = new FileStream(csvFilePath, FileMode.Open);
            StreamReader sr = new StreamReader(fs);

            string lineOfTxtFile = sr.ReadLine();

            //  CHANGE THIS if your seperator is not ^ (like, ususally its ",")
            string[] splitter = { "," };


            string[] fieldNames = lineOfTxtFile.Split(splitter, StringSplitOptions.RemoveEmptyEntries);

            lineOfTxtFile = sr.ReadLine();
            string temp = lineOfTxtFile;
            string[] dataItems = temp.Split(splitter, StringSplitOptions.None);
            string[] fieldTypes = Classify(dataItems);
            fs.Close();
            
            CreateTable("TestTable", fieldNames, fieldTypes);
            
            /*  So, because we have to call QueuedTask.Run to add the schema, this is forced into being an async call, which means that
             *   if I let it return, execution continues ... And there is nothing worse than trying to put data into a schema that isn't 
             *   created yet (HINT:  NEED A SYNC function here), so to get around that, the load data call is at the end of the add schema procedure.
             *   This forces schema creation to finsh before loading data.  I also chose to create a new streamreader/filestream in the data load procedure
             *   because passing it around while debugging was getting confusing.
             */
            var result = Task.Run(() => AddSchema("TestTable", fieldNames, fieldTypes, splitter, csvFilePath));
            if (result.Result.ToString() == "ok") System.Windows.MessageBox.Show("Done");
        }


        public async Task<string> LoadData(string[] splitter, string[] fieldTypes, string csvFilePath)
        {
            await QueuedTask.Run(() => 
            {
                string lineOfTxtFile = "";
                FileStream fs = new FileStream(csvFilePath, FileMode.Open);
                StreamReader sr = new StreamReader(fs);

                lineOfTxtFile = sr.ReadLine();  // skip the title line
                lineOfTxtFile = sr.ReadLine();

                using (Geodatabase projectWorkspace = new Geodatabase(new FileGeodatabaseConnectionPath(new Uri(Project.Current.DefaultGeodatabasePath))))
                using (Table table = projectWorkspace.OpenDataset<Table>("TestTable"))
                using (RowBuffer rowBuffer = table.CreateRowBuffer())
                {
                    while (lineOfTxtFile != null)
                    {                        
                        string[] dataItems = lineOfTxtFile.Split(splitter, StringSplitOptions.None);

                        for (int i = 0; i < dataItems.Length; i++)
                        {
                            switch (fieldTypes[i])
                            {
                                case "Text":
                                    rowBuffer[i + 1] = dataItems[i].Trim();
                                    break;
                                case "Short":
                                    if (dataItems[i].Length > 0)
                                    {
                                        if (dataItems[i].Contains(".")) rowBuffer[i + 1] = Int16.Parse(dataItems[i].Substring(0, dataItems[i].IndexOf('.')));
                                        else rowBuffer[i + 1] = Int16.Parse(dataItems[i]);
                                    }
                                    else rowBuffer[i + 1] = null;
                                    break;

                                case "Long":
                                    if (dataItems[i].Length > 0)
                                    {
                                        if (dataItems[i].Contains(".")) rowBuffer[i + 1] = int.Parse(dataItems[i].Substring(0, dataItems[i].IndexOf('.')));
                                        else rowBuffer[i + 1] = int.Parse(dataItems[i]);
                                    }
                                    else rowBuffer[i + 1] = null;
                                    break;

                                case "Date":
                                    if (dataItems[i].Length > 0) rowBuffer[i + 1] = DateTime.Parse(dataItems[i]);
                                    else rowBuffer[i + 1] = null;
                                    break;

                                case "Double":
                                    if (dataItems[i].Length > 0) rowBuffer[i + 1] = double.Parse(dataItems[i]);
                                    else rowBuffer[i + 1] = null;
                                    break;
                                default:
                                    break;
                            }                            
                        }
                        table.CreateRow(rowBuffer);
                        lineOfTxtFile = sr.ReadLine();
                    }
                    rowBuffer.Dispose();
                }                        
            });
            return "ok";
        }



        /*  This routine is custom designed to produce the schema required by the table.  The concept is simple:  load your csv file in Excel, delete the data,
         *  number the fields starting at 0, and then group them using the index into the right type.  Way simpler than manually typing each field.
         *  
         */

        private string[] Classify(string[] dataItems)
        {
            string[] fieldTypes = new string[dataItems.Length];
            
            for (int i = 0; i < dataItems.Length; i ++)
            {
                switch (i)
                {

                    case 12:
                        fieldTypes[i] = "Double";
                            break;
                    case 0:
                        fieldTypes[i] = "Date";
                        break;

                    case 13:
                        fieldTypes[i] = "Short";
                        break;
                    
                    case 14:
                        fieldTypes[i] = "Long";
                        break;
                    
                    default:
                        fieldTypes[i] = "Text";
                        break;
                }
            }
            return fieldTypes;
        }

        protected async Task<string> AddSchema(string tableName, string[] fieldNames, string[] fieldTypes, string[] splitter, string csvFilePath)
        {
            // loop to create table schema
            /*  Note field properties are: tableName, fieldName, DataType: [Text, Short, Long, Float, Date, Blob, Raster Guid, Double], 
             *  Domain, Default, fieldLength [number of characters ie, 32], AliasName, "NULLABLE", "NON_REQUIRED", null
             *  ref: https://community.esri.com/thread/246461-arcgis-pro-sdk-create-and-add-new-standalone-table
             */
            
            for (int i = 0; i < fieldNames.Length; i++)
            {
                IReadOnlyList<string> addFieldParams = Geoprocessing.MakeValueArray(new object[] { tableName, fieldNames[i], fieldTypes[i], 
                    null, null, 255, "", "NULLABLE", "NON_REQUIRED", null });
                IGPResult result = await  Geoprocessing.ExecuteToolAsync("management.AddField", addFieldParams);
                if (result.IsFailed)
                {
                    ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show("Unable to create Field");
                } 
            }

            var dataLoad = LoadData(splitter, fieldTypes, csvFilePath);
            return "ok";            
        }

        protected async void CreateTable(string tableName, string[] fieldNames, string[] fieldTypes)
        {
            //  Using the default geodatabase here. You can add a dialog and set yours if you want to.

            

            IReadOnlyList<string> createParams = Geoprocessing.MakeValueArray(new object[] { Project.Current.DefaultGeodatabasePath, tableName, null, null });
            IGPResult result = await Geoprocessing.ExecuteToolAsync("management.CreateTable", createParams);
            if (result.IsFailed)
            {
                ArcGIS.Desktop.Framework.Dialogs.MessageBox.Show("Unable to create table");
            }           
            
           
        }
        
    }
}
