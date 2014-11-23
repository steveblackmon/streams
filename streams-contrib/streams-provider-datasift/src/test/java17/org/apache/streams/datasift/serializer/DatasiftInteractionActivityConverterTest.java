package org.apache.streams.datasift.serializer;

import org.apache.streams.datasift.Datasift;
import org.junit.Before;
import org.junit.Test;

import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatasiftInteractionActivityConverterTest extends DatasiftActivityConverterTest {

    @Before
    @Override
    public void initSerializer() {
        SERIALIZER = new DatasiftInteractionActivityConverter();
    }

    @Test
    @Override
    public void testConversion() throws Exception {
        Scanner scanner = new Scanner(DatasiftInteractionActivityConverterTest.class.getResourceAsStream("/rand_sample_datasift_json.txt"));
        String line = null;
        while(scanner.hasNextLine()) {
            try {
                line = scanner.nextLine();
                Datasift item = MAPPER.readValue(line, Datasift.class);
                testConversion(item);
                String json = MAPPER.writeValueAsString(item);
                testDeserNoNull(json);
                testDeserNoAddProps(json);
            } catch (Exception e) {
                System.err.println(line);
                throw e;
            }
        }
    }

}