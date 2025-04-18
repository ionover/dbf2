package org.example;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;

public class ShpDbfConverter {

    /**
     * Конвертирует все поля TIMESTAMP_DBASE7 ('@') в DATE ('D') формата YYYYMMDD.
     * Исходный файл: parcels_new_1_df2a__7829Polygon.dbf
     * Результат:        output.dbf
     */
    public static void convertTimestampToDate(String inputDbfPath,
                                              String outputDbfPath) throws Exception {
        // Для shapefile-DBF часто используется CP866; при необходимости меняйте на CP1251 и т.п.
        Charset charset = Charset.forName("CP866");

        try (InputStream  inStream = new FileInputStream(inputDbfPath);
             DBFReader    reader   = new DBFReader(inStream, charset);
             FileOutputStream fos   = new FileOutputStream(outputDbfPath)) {

            // 1) Собираем новый список полей, конвертируя TIMESTAMP_DBASE7 → DATE
            List<DBFField> newFields = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField oldField = reader.getField(i);
                DBFDataType type = oldField.getType();

                if (type == DBFDataType.TIMESTAMP_DBASE7) {
                    // код '@' в исходнике, запись в DATE
                    DBFField df = new DBFField();
                    df.setName(oldField.getName());
                    df.setType(DBFDataType.DATE);
                    newFields.add(df);
                } else {
                    // всё остальное — без изменений
                    newFields.add(oldField);
                }
            }

            // 2) Инициализируем писатель и задаём ему уже скорректированный список полей
            DBFWriter writer = new DBFWriter(fos, charset);
            writer.setFields(newFields.toArray(new DBFField[0]));  // теперь нет TIMESTAMP_DBASE7 → исключим ошибку :contentReference[oaicite:0]{index=0}

            // 3) Копируем все записи: reader.nextRecord() вернёт java.util.Date для TIMESTAMP_DBASE7,
            //    writer.addRecord() автоматически запишет это в формате YYYYMMDD в поле DATE.
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                writer.addRecord(row);
            }

            writer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath  = "parcels_new_1_df2a__7829Polygon.dbf";
        String outputPath = "output.dbf";

        System.out.println("Читаем из: " + inputPath);
        convertTimestampToDate(inputPath, outputPath);
        System.out.println("Готово, записано в: " + outputPath);
    }
}
