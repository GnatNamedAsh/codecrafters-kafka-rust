
use std::io;

use crate::kafka::server::Connection;

use crate::kafka::topic::Topic;

struct ApiVersion {
  api_key: i16,
  min_version: u16,
  max_version: u16,
}

fn get_supported_apis() -> Vec<ApiVersion> {
  vec![
    ApiVersion {
      api_key: 18,
      min_version: 0,
      max_version: 4,
    },
    ApiVersion {
      api_key: 75,
      min_version: 0,
      max_version: 0,
    },
  ]
}

// authorized_operations bitmask
// bit 1 - UNKNOWN
// bit 2 - ANY
// bit 3 - READ
// bit 4 - WRITE
// bit 5 - CREATE
// bit 6 - DELETE
// bit 7 - ALTER
// bit 8 - DESCRIBE
// bit 9 - CLUSTER_ACTION
// bit 10 - DESCRIBE_CONFIGS
// bit 11 - ALTER_CONFIGS
// bit 12 - IDEMPOTENT_WRITE
// bit 13 - CREATE_TOKENS
// bit 14 - DESCRIBE_TOKENS


// https://kafka.apache.org/26/protocol.html#The_Messages_ApiVersions
pub fn handle_api_key_18(connection: &mut Connection) -> Result<([u8; 1016], i32), io::Error> {
  let tag_buffer = &0_i8.to_be_bytes();
  let mut response_body: [u8; 1016] = [0x00; 1016];
  if connection.api_version > 4 {
    response_body[0..2].copy_from_slice(&35_i16.to_be_bytes());
    return Ok((response_body, 2));
  }
  
  let supported_apis = get_supported_apis();

  // There needs to be a tag buffer between keys, and a tag buffer after the throttle_time_ms
  // error code
  response_body[0..2].copy_from_slice(&0_i16.to_be_bytes());
  // num_api_keys
  let num_api_keys = supported_apis.len() as i8;
  // kafka takes the length of the array + 1 as the number of api keys
  response_body[2..3].copy_from_slice(&(num_api_keys + 1).to_be_bytes());
  // api_keys ->
  let mut start_index = 3;
  for api in supported_apis {
    response_body[start_index..start_index + 2].copy_from_slice(&api.api_key.to_be_bytes());
    start_index += 2;
    response_body[start_index..start_index + 2]
        .copy_from_slice(&api.min_version.to_be_bytes());
    start_index += 2;
    response_body[start_index..start_index + 2]
        .copy_from_slice(&api.max_version.to_be_bytes());
    start_index += 2;
    response_body[start_index..start_index + 1].copy_from_slice(tag_buffer);
    start_index += 1;
  }

  // throttle_time_ms
  response_body[start_index..start_index + 4].copy_from_slice(&0_i32.to_be_bytes());
  start_index += 4;
  // tag buffer
  response_body[start_index..start_index + 1].copy_from_slice(tag_buffer);
  Ok((response_body, (start_index + 1) as i32))
}

// we're just going to mutate the response buffer here
// and then return the last location in the buffer we wrote to
fn handle_api_key_75_unknown_topic(response_buffer: &mut [u8; 1016], start_index: usize, topic: Topic) -> Result<usize, io::Error> {
  let mut end_index = start_index;
  let topic_name = topic.name;
  // we'll handle the length of the array outside of this function - this is just for putting in an unknown topic when the upstream function runs into an unknown topic
  // topic -> 
  //   error_code (int16) - 3 is for unknown topic
  //   topic_name -> topic_name_length (length of the string + 1) (int8) + topic_name (string, topic_name_length bytes)
  //   topic_id 16 bytes UUID (int8 array) - all 0 as null for not found
  //   is_internal (bool) - false for now
  //   partitiions_array_length (int8) -> length of the array + 1 (0 or 1 means empty array)
  //   authorized_operations (int32) - bitmask of operations
  
  // error code - UNKNOWN_TOPIC_OR_PARTITION
  response_buffer[end_index..end_index + 2].copy_from_slice(&3_i16.to_be_bytes());
  end_index += 2;
  // topic name length (length of the string + 1)
  let topic_name_length = 1 + topic_name.len() as i8;
  response_buffer[end_index..end_index + 1].copy_from_slice(&topic_name_length.to_be_bytes());
  end_index += 1;
  // topic name
  response_buffer[end_index..end_index + topic_name_length as usize].copy_from_slice(topic_name.as_bytes());
  end_index += topic_name_length as usize;
  // topc id
  response_buffer[end_index..end_index + 16].copy_from_slice(&[0; 16]);
  end_index += 16;
  // is_internal
  response_buffer[end_index..end_index + 1].copy_from_slice(&0_i8.to_be_bytes());
  end_index += 1;
  // partitions_array_length
  response_buffer[end_index..end_index + 1].copy_from_slice(&1_i8.to_be_bytes());
  end_index += 1;
  // authorized_operations
  // 0000 1101 1111 1000 is the bitmask for the operations expected to be returned
  response_buffer[end_index..end_index + 4].copy_from_slice(&0x0d_f8_i32.to_be_bytes());
  end_index += 4;

  Ok(end_index)
}

// fn handle_api_key_75_known_topic(response_buffer: &mut [u8; 1016], start_index: usize, topic_name: String) -> Result<usize, io::Error> {
//   let mut end_index = start_index;
//   // we'll handle the length of the array outside of this function - this is just for putting in an unknown topic when the upstream function runs into an unknown topic
//   // topic -> 
//   //   error_code (int16) - 3 is for unknown topic
//   //   topic_name -> topic_name_length (length of the string + 1) (int8) + topic_name (string, topic_name_length bytes)
//   //   topic_id 16 bytes UUID (int8 array) - all 0 as null for not found
//   //   is_internal (bool) - false for now
//   //   partitiions_array_length (int8) -> length of the array + 1 (0 or 1 means empty array)
//   //   authorized_operations (int32) - bitmask of operations
  
//   // error code - UNKNOWN_TOPIC_OR_PARTITION
//   response_buffer[end_index..end_index + 2].copy_from_slice(&3_i16.to_be_bytes());
//   end_index += 2;

//   Ok(end_index)
// }

fn parse_body(connection: &mut Connection) -> Result<(), io::Error> {
  // take the body_length, and then parse the body for topics.
  // the first byte is the array length where 2 means 1 topic in the array
  let topic_array_length = (connection.body[0] as i8) - 1;
  let mut current_index = 1;
  // e.g. topic_array_length value is 2, which means a length of 1 topic,, so we'll loop once
  for _ in 0..topic_array_length {
    let topic_name_length = (connection.body[current_index] as i8) - 1; // length of the topic name + 1 so 4 is 3 bytes
    current_index += 1; // e.g. now at byte 3 (index 2)
    // e.g. topic_name_length is 3, so no we're taking bytes 3-5 (index 2-4 inclusive), so slice is 2..5
    let topic_name = match String::from_utf8(connection.body[current_index..current_index + topic_name_length as usize].to_vec()) {
      Ok(topic_name) => topic_name,
      Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid topic name")),
    };
    connection.topics.push(Topic { name: topic_name });
    // e.g. topic_name_length is 3, so now we move the current index up by 3, which is index 5 (byte 6)
    current_index += topic_name_length as usize;
    // tag buffer - ignore - so index is now 6 (byte 7)
    current_index += 1;
  }
  // after reading the topics, the request has a partition limit for the response which is a 4 byte int
  // e.g. byte 7-10 is the partition limit (index 6-9, so 6..10)
  connection.partition_limit = i32::from_be_bytes(connection.body[current_index..current_index + 4].try_into().unwrap());
  current_index += 4;
  // cursor - ignore for now since we're not returning any partitions, (index 10, byte 11)
  current_index += 1;
  // tag buffer - ignore for now (index 11, byte 12)
  current_index += 1;
  // verify that we've read the entire body
  if current_index != connection.body_length {
    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid body length"));
  }
  Ok(())
}

pub fn handle_api_key_75 (connection: &mut Connection) -> Result<([u8; 1016], i32), io::Error> {
  let mut response_body: [u8; 1016] = [0x00; 1016];
  let mut start_index = 0;
  let tag_buffer = &0_i8.to_be_bytes();
  // tag buffer
  response_body[0..1].copy_from_slice(tag_buffer);
  start_index += 1;
  // throttle_time_ms
  response_body[start_index..start_index + 4].copy_from_slice(&0_i32.to_be_bytes());
  start_index += 4;

  // From here, we start parsing the body of the request for topics
  parse_body(connection)?;
  // we need to take a look at the body of the request here
  // the body is a list of topics, each with a tag buffer
  // we then need to return the list of topics
  // the bytes are as follows if the topics requested are unknown:
  // array length (int8) - length of the array + 1
  // topics (we'll handle this outside of this function since we'll need to do)
  // for now, we're just going to return the unknown topic response and deal with parsing
  // the body and finding partitions later

  // tag buffer (int8)
  // next_cursor (int8) - 0xff null value
  // tag buffer
  
  // we'll want to add a check here to see if the topics requested are know,
  // if not, we'll want to generate a response with the unknown topics
  // if they are known, we'll want to generate a response with the known topics
  // we'll want to return the response body, and the length of the response body

  Ok((response_body, 2))
}