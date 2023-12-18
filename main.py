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
                "topic": source_description["topic"],
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


def main():
    streams_need_to_create = []

    for ods_stream in ODS_STREAMS:
        for stream_name in get_stream_flow(ods_stream)[::-1]:
            if "ETL" in stream_name:
                streams_need_to_create.append(stream_name)
                break

            if "ODS" in stream_name:
                streams_need_to_create.append(stream_name)
                break

    for stream_info in ALL_STREAMS_AND_TOPICS:
        for stream_need_to_create in streams_need_to_create:
            if stream_need_to_create == stream_info["name"]:
                print("-- {}".format(stream_need_to_create))
                print(stream_info["statement"])
                print("\n")


ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

if __name__ == "__main__":
    main()
