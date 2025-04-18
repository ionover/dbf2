package org.example;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;

public class ShpDbfConverter {

    /**
     * Заменяем все поля TIMESTAMP_DBASE7 ('@') на CHARACTER(8) с форматом ddMMyyyy.
     * Чтение/запись в UTF-8.
     */
    public static void convertTimestampToCharDate(String inputDbfPath,
                                                  String outputDbfPath) throws Exception {
        Charset charset = Charset.forName("UTF-8");
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        try (InputStream inStream = new FileInputStream(inputDbfPath);
             DBFReader reader = new DBFReader(inStream, charset);
             FileOutputStream fos = new FileOutputStream(outputDbfPath)) {

            // 1) Собираем описание полей
            List<DBFField> newFields = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField old = reader.getField(i);
                if (old.getType() == DBFDataType.TIMESTAMP_DBASE7) {
                    DBFField fld = new DBFField();
                    fld.setName(old.getName());
                    fld.setType(DBFDataType.CHARACTER);
                    fld.setFieldLength(10);          // ddMMyyyy
                    newFields.add(fld);
                } else {
                    newFields.add(old);
                }
            }

            // 2) Создаём DBFWriter и задаём все поля
            DBFWriter writer = new DBFWriter(fos, charset);
            writer.setFields(newFields.toArray(new DBFField[0]));

            // 3) Копируем записи, переводя Date → "ddMMyyyy"
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                for (int i = 0; i < row.length; i++) {
                    if (reader.getField(i).getType() == DBFDataType.TIMESTAMP_DBASE7
                            && row[i] instanceof Date) {
                        row[i] = sdf.format((Date) row[i]);
                    }
                }
                writer.addRecord(row);
            }

            writer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath  = "parcels_new_1_df2a__7829Polygon.dbf";
        String outputPath = "output.dbf";

        System.out.println("Читаем из:  " + inputPath + " (UTF-8)");
        convertTimestampToCharDate(inputPath, outputPath);
        System.out.println("Готово, записано в: " + outputPath);
    }
}
