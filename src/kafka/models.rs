pub struct TopicRecord {
    frame_version: i8,
    record_type: i8,
    record_version: i8,
    topic_name: String,
    topic_uuid: [u8; 16],
}
