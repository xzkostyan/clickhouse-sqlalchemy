from enum import Enum

from .base import Engine


class KafkaFormat(Enum):
    TabSeparated = "TabSeparated"
    TabSeparatedRaw = "TabSeparatedRaw"
    TabSeparatedWithNames = "TabSeparatedWithNames"
    TabSeparatedWithNamesAndTypes = "TabSeparatedWithNamesAndTypes"
    TabSeparatedRawWithNames = "TabSeparatedRawWithNames"
    TabSeparatedRawWithNamesAndTypes = "TabSeparatedRawWithNamesAndTypes"
    Template = "Template"
    TemplateIgnoreSpaces = "TemplateIgnoreSpaces"
    CSV = "CSV"
    CSVWithNames = "CSVWithNames"
    CSVWithNamesAndTypes = "CSVWithNamesAndTypes"
    CustomSeparated = "CustomSeparated"
    CustomSeparatedWithNames = "CustomSeparatedWithNames"
    CustomSeparatedWithNamesAndTypes = "CustomSeparatedWithNamesAndTypes"
    Values = "Values"
    JSON = "JSON"
    JSONAsString = "JSONAsString"
    JSONAsObject = "JSONAsObject"
    JSONStrings = "JSONStrings"
    JSONColumns = "JSONColumns"
    JSONColumnsWithMetadata = "JSONColumnsWithMetadata"
    JSONCompact = "JSONCompact"
    JSONCompactColumns = "JSONCompactColumns"
    JSONEachRow = "JSONEachRow"
    JSONStringsEachRow = "JSONStringsEachRow"
    JSONCompactEachRow = "JSONCompactEachRow"
    JSONCompactEachRowWithNames = "JSONCompactEachRowWithNames"
    JSONCompactEachRowWithNamesAndTypes = "JSONCompactEachRowWithNamesAndTypes"
    JSONCompactStringsEachRow = "JSONCompactStringsEachRow"
    JSONCompactStringsEachRowWithNames = "JSONCompactStringsEachRowWithNames"
    JSONCompactStringsEachRowWithNamesAndTypes = (
        "JSONCompactStringsEachRowWithNamesAndTypes"
    )
    JSONObjectEachRow = "JSONObjectEachRow"
    BSONEachRow = "BSONEachRow"
    TSKV = "TSKV"
    Protobuf = "Protobuf"
    ProtobufSingle = "ProtobufSingle"
    ProtobufList = "ProtobufList"
    Avro = "Avro"
    AvroConfluent = "AvroConfluent"
    Parquet = "Parquet"
    ParquetMetadata = "ParquetMetadata"
    Arrow = "Arrow"
    ArrowStream = "ArrowStream"
    ORC = "ORC"
    One = "One"
    Npy = "Npy"
    RowBinary = "RowBinary"
    RowBinaryWithNames = "RowBinaryWithNames"
    RowBinaryWithNamesAndTypes = "RowBinaryWithNamesAndTypes"
    RowBinaryWithDefaults = "RowBinaryWithDefaults"
    Native = "Native"
    CapnProto = "CapnProto"
    LineAsString = "LineAsString"
    Regexp = "Regexp"
    RawBLOB = "RawBLOB"
    MsgPack = "MsgPack"
    MySQLDump = "MySQLDump"
    DWARF = "DWARF"
    Form = "Form"


class Kafka(Engine):
    __visit_name__ = "kafka"

    # opting to represent required settings as engine parameters
    def __init__(
        self,
        kafka_broker_list,
        kafka_topic_list,
        kafka_group_name,
        kafka_format,
        **settings,
    ):
        """See
        https://clickhouse.com/docs/engines/table-engines/integrations/kafka
        for more information on allowed settings for KafkaEngine tables.

        Args:
            kafka_broker_list (list[str]): list of brokers
            kafka_topic_list (list[str]): list of Kafka topics
            kafka_group_name (str):  a group of Kafka consumers. Reading
                margins are tracked for each group separately. If you do not
                want messages to be duplicated in the cluster, use the same
                group name everywhere.
            kafka_format (str): message format. Uses the same notation as the
                SQL FORMAT function, such as JSONEachRow. For more information,
                see the Formats section
                https://clickhouse.com/docs/interfaces/formats
        """
        settings["kafka_broker_list"] = ",".join(kafka_broker_list)
        settings["kafka_topic_list"] = ",".join(kafka_topic_list)
        settings["kafka_group_name"] = kafka_group_name
        # for validation of allowed formats
        settings["kafka_format"] = KafkaFormat(kafka_format).value
        self.settings = settings
        super().__init__()
