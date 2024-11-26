using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;

class Program
{
    static void Main(string[] args)
    {
        string csvFilePath = @"C:\Users\Admin\Desktop\Data.csv";
        string connectionString = "Server=localhost;Database=Data_Test;Trusted_Connection=True;";
        string tableName = "DTB";

        List<string[]> csvData = new List<string[]>();

        // 1. Đọc file CSV
        using (var reader = new StreamReader(csvFilePath))
        {
            string headerLine = reader.ReadLine(); // Đọc dòng tiêu đề
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                if (!string.IsNullOrWhiteSpace(line))
                {
                    string[] values = line.Split(',');
                    csvData.Add(values);
                }
            }
        }



        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            foreach (var record in csvData)
            {

                string noModel = record[0];
                string rawDateTime = record[1];
                string judge = record[2];

                // Chuyển đổi Date_time
                DateTime dateTime;

                string datePart = rawDateTime.Substring(0, 8);
                string timePart = rawDateTime.Substring(9);
                string formattedDateTime = $"{datePart} {timePart}";
                dateTime = DateTime.ParseExact(formattedDateTime, "yyyyMMdd HHmm", CultureInfo.InvariantCulture);



                // Lấy dữ liệu hiện tại từ SQL Server
                string selectQuery = $@"
                        SELECT TOP 1 No_Model, Date_time, judge
                        FROM [{tableName}]
                        WHERE No_Model = @NoModel
                        ORDER BY Date_time DESC";

                SqlCommand selectCmd = new SqlCommand(selectQuery, connection);
                selectCmd.Parameters.AddWithValue("@NoModel", noModel);

                SqlDataReader readerDb = selectCmd.ExecuteReader();

                bool exists = false;
                DateTime existingDateTime = DateTime.MinValue;
                while (readerDb.Read())
                {
                    exists = true;
                    existingDateTime = readerDb.GetDateTime(1);
                }
                readerDb.Close();

                if (!exists)
                {
                    // Chèn dữ liệu mới
                    string insertQuery = $@"
                            INSERT INTO [{tableName}] (No_Model, Date_time, judge)
                            VALUES (@NoModel, @DateTime, @Judge)";

                    SqlCommand insertCmd = new SqlCommand(insertQuery, connection);
                    insertCmd.Parameters.AddWithValue("@NoModel", noModel);
                    insertCmd.Parameters.AddWithValue("@DateTime", dateTime);
                    insertCmd.Parameters.AddWithValue("@Judge", judge);
                    insertCmd.ExecuteNonQuery();
                }
                else if (dateTime > existingDateTime)
                {
                    // Cập nhật dữ liệu nếu mới hơn
                    string updateQuery = $@"
                            UPDATE [{tableName}]
                            SET Date_time = @DateTime, judge = @Judge
                            WHERE No_Model = @NoModel";

                    SqlCommand updateCmd = new SqlCommand(updateQuery, connection);
                    updateCmd.Parameters.AddWithValue("@NoModel", noModel);
                    updateCmd.Parameters.AddWithValue("@DateTime", dateTime);
                    updateCmd.Parameters.AddWithValue("@Judge", judge);
                    updateCmd.ExecuteNonQuery();
                }
            }

            connection.Close();
        }

        Console.WriteLine("Đồng bộ dữ liệu hoàn tất!");
    }

}

