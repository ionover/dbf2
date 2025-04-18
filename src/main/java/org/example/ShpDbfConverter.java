package org.example;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;

public class ShpDbfConverter {

    /**
     * Заменяем все поля TIMESTAMP_DBASE7 ('@') на DATE ('D').
     * Чтение/запись в UTF-8.
     */
    public static void convertTimestampToDateField(String inputDbfPath,
                                                   String outputDbfPath) throws Exception {
        Charset charset = Charset.forName("UTF-8");

        try (InputStream inStream = new FileInputStream(inputDbfPath);
             DBFReader reader = new DBFReader(inStream, charset);
             FileOutputStream fos = new FileOutputStream(outputDbfPath)) {

            // 1) Собираем описание полей, меняя TIMESTAMP на DATE
            List<DBFField> newFields = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField old = reader.getField(i);
                if (old.getType() == DBFDataType.TIMESTAMP_DBASE7) {
                    DBFField fld = new DBFField();
                    fld.setName(old.getName());
                    fld.setType(DBFDataType.DATE);
                    fld.setFieldLength(8);   // фиксированная длина для DATE-поля
                    newFields.add(fld);
                } else {
                    newFields.add(old);
                }
            }

            // 2) Создаём DBFWriter и задаём все поля
            DBFWriter writer = new DBFWriter(fos, charset);
            writer.setFields(newFields.toArray(new DBFField[0]));

            // 3) Копируем записи — Date остаётся Date
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                // Для полей-конвертеров можно при необходимости обработать null-ы,
                // но в целом передаём Date напрямую в DATE-поле.
                writer.addRecord(row);
            }

            writer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath  = "src/main/parcels_new_1_df2a__7829Polygon.dbf";
        String outputPath = "output.dbf";

        System.out.println("Читаем из:  " + inputPath + " (UTF-8)");
        convertTimestampToDateField(inputPath, outputPath);
        System.out.println("Готово, записано в: " + outputPath);
    }
}
