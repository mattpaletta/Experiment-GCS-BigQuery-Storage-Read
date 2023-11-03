#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
// Include avro.
#include "Generic.hh"
#include "Compiler.hh"
#include <iostream>

static void ProcessRowsInAvroFormat(const ::google::cloud::bigquery::storage::v1::AvroSchema & schema, const ::google::cloud::bigquery::storage::v1::AvroRows& rows, int64_t count) {
    // Code to deserialize avro rows should be added here.
    const avro::ValidSchema vs = avro::compileJsonSchemaFromString(schema.schema());
    std::istringstream iss(rows.serialized_binary_rows(), std::ios::binary);
    std::unique_ptr<avro::InputStream> in = avro::istreamInputStream(iss);

    avro::DecoderPtr d = avro::validatingDecoder(vs, avro::binaryDecoder());
    avro::GenericReader gr(vs, d);
    d->init(*in);

    avro::GenericDatum datum(vs);

    for (auto i = 0; i < count; i++) {
        gr.read(*d, datum, vs);

        if (datum.type() == avro::AVRO_RECORD) {
            const avro::GenericRecord &r = datum.value<avro::GenericRecord>();
            std::cout << "Field-count: " << r.fieldCount() << std::endl;
            for (auto i = 0; i < r.fieldCount(); i++) {
                const avro::GenericDatum &f0 = r.fieldAt(i);
                if (f0.type() == avro::AVRO_STRING) {
                    std::cout << "string: " << f0.value<std::string>() << " ";
                } else if (f0.type() == avro::AVRO_INT) {
                    std::cout << "int: " << f0.value<int>() << " ";
                } else if (f0.type() == avro::AVRO_LONG) {
                    std::cout << "long: " << f0.value<long>() << " ";
                } else {
                    std::cout << f0.type() << " ";
                }
            }
            std::cout << std::endl;
        }

        gr.drain();
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 3) {
            std::cerr << "Usage: " << argv[0] << " <project-id> <table-name>\n";
            return 1;
        }

        // project_name should be in the format "projects/<your-gcp-project>"
        std::string const project_name = "projects/" + std::string(argv[1]);
        // table_name should be in the format:
        // "projects/<project-table-resides-in>/datasets/<dataset-table_resides-in>/tables/<table
        // name>" The project values in project_name and table_name do not have to be
        // identical.
        std::string const table_name = argv[2];

        // Create a namespace alias to make the code easier to read.
        namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
        constexpr int kMaxReadStreams = 1;
        // Create the ReadSession.
        auto client = bigquery_storage::BigQueryReadClient(bigquery_storage::MakeBigQueryReadConnection());        
        ::google::cloud::bigquery::storage::v1::ReadSession read_session;
        read_session.set_data_format(google::cloud::bigquery::storage::v1::DataFormat::AVRO);
        read_session.set_table(table_name);
        auto session = client.CreateReadSession(project_name, read_session, kMaxReadStreams);
        if (!session) throw std::move(session).status();

        // Read rows from the ReadSession.
        constexpr int kRowOffset = 0;
        auto read_rows = client.ReadRows(session->streams(0).name(), kRowOffset);

        std::int64_t num_rows = 0;
        for (auto const& row : read_rows) {
            if (row.ok()) {
                num_rows += row->row_count();
                ProcessRowsInAvroFormat(session->avro_schema(), row->avro_rows(), row->row_count());
            }
        }

        std::cout << num_rows << " rows read from table: " << table_name << "\n";
        return 0;
    } catch (const google::cloud::Status& status) {
        std::cerr << "google::cloud::Status thrown: " << status << "\n";
        return 1;
    } catch (const avro::Exception& ex) {
        std::cerr << "avro::Exception thrown: " << ex.what() << "\n";
        return 1;
    }
}