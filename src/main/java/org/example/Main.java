package org.example;

import java.io.FileInputStream;
import java.io.InputStream;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFDataType;

public class Main {
    public static void main(String[] args) throws Exception {
        String dbfFile = "parcels_new_1_df2a__7829Polygon.dbf";
        try (InputStream in = new FileInputStream(dbfFile)) {
            DBFReader reader = new DBFReader(in);
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField field = reader.getField(i);
                DBFDataType dtype = field.getType();
                char code = dtype.getCharCode();   // получить 'C', 'N', 'D', '@' и т.д. :contentReference[oaicite:0]{index=0}

                System.out.printf(
                        "Field name: %s, Type code: %c%n",
                        field.getName(),
                        code
                );
            }
        }
    }
}
