import io
import avro.schema
import avro.io


def load_avro_schema(schema_path):
    return avro.schema.parse(open(schema_path).read())
    
def avro_encode(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()