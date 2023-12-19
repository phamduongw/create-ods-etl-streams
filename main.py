from ksqldb_services import list_streams_extended

with open("ods_streams.txt", "r") as file:
    ODS_STREAMS = file.read().split("\n")


def get_all_streams_and_topics():
    all_streams_and_topics = []

    source_descriptions = list_streams_extended()[0]["sourceDescriptions"]

    for source_description in source_descriptions:
        sinks = []

        for read_query in source_description["readQueries"]:
            for sink in read_query["sinks"]:
                sinks.append(sink)

        all_streams_and_topics.append(
            {
                "name": source_description["name"],
                "sinks": sinks,
                "statement": source_description["statement"],
            }
        )

    return all_streams_and_topics


def get_stream_flow(ods_stream):
    stream_flow = []

    def get_stream_flow_item(ods_stream):
        stream_flow.append(ods_stream)

        for stream_info in ALL_STREAMS_AND_TOPICS:
            for sink in stream_info["sinks"]:
                if sink == ods_stream:
                    get_stream_flow_item(stream_info["name"])

    get_stream_flow_item(ods_stream)

    return stream_flow


def print_to_console(stream_name, statement):
    print("-- {}\n{}\n\n".format(stream_name, statement))


def get_statement_of_stream(is_exist, stream_name):
    if is_exist:
        for stream_info in ALL_STREAMS_AND_TOPICS:
            if stream_info["name"] == stream_name:
                print_to_console(stream_name, stream_info["statement"])
    else:
        print_to_console(stream_name, "ERROR!")


def main():
    for ods_stream in ODS_STREAMS:
        if ods_stream:
            stream_flow = get_stream_flow(ods_stream.strip())[::-1]
            for stream_name in stream_flow:
                if len(stream_flow) == 1:
                    get_statement_of_stream(False, stream_name)
                    break

                if "ETL" in stream_name:
                    get_statement_of_stream(True, stream_name)
                    break

                if "ODS" in stream_name:
                    get_statement_of_stream(True, stream_name)
                    break


ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

if __name__ == "__main__":
    main()
