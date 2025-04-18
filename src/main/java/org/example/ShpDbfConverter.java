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
     * Конвертирует все поля TIMESTAMP_DBASE7 ('@') в DATE ('D') формата YYYYMMDD,
     * и читает/пишет строки в UTF-8.
     *
     * Исходник: parcels_new_1_df2a__7829Polygon.dbf
     * Результат: output.dbf
     */
    public static void convertTimestampToDate(String inputDbfPath,
                                              String outputDbfPath) throws Exception {
        // Заменили CP866 на UTF-8:
        Charset charset = Charset.forName("UTF-8");

        try (InputStream  inStream = new FileInputStream(inputDbfPath);
             DBFReader    reader   = new DBFReader(inStream, charset);
             FileOutputStream fos   = new FileOutputStream(outputDbfPath)) {

            // 1) Новый список полей: TIMESTAMP_DBASE7 → DATE
            List<DBFField> newFields = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField oldField = reader.getField(i);
                if (oldField.getType() == DBFDataType.TIMESTAMP_DBASE7) {
                    DBFField df = new DBFField();
                    df.setName(oldField.getName());
                    df.setType(DBFDataType.DATE);
                    newFields.add(df);
                } else {
                    newFields.add(oldField);
                }
            }

            // 2) Писатель с UTF-8 и обновлёнными полями
            DBFWriter writer = new DBFWriter(fos, charset);
            writer.setFields(newFields.toArray(new DBFField[0]));

            // 3) Копируем записи: Date автоматически → YYYYMMDD
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

        System.out.println("Читаем из: " + inputPath + " в UTF-8");
        convertTimestampToDate(inputPath, outputPath);
        System.out.println("Готово, записано в: " + outputPath);
    }
}
