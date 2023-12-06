import base64
import json
from datetime import datetime
import re
import pytz
import uuid


utc_tz = pytz.timezone('UTC')
thai_tz = pytz.timezone('Asia/Bangkok')


thai_pattern = re.compile(r"[\u0E00-\u0E7F]")
eng_pattern = re.compile(r"[a-zA-Z]")
channel_lst = []
flow_regex_lst = []
logger = None

QUICK_REPLY = 'QUICK_REPLY'
TEXT = 'TEXT'
ATTACHMENT = 'ATTACHMENT'
IMAGE = 'IMAGE'
UNKNOWN_ATTACH = 'UNKNOWN_ATTACHMENT'
BUTTON_TEMPLATE = 'BUTTON_TEMPLATE'
GENERIC_TEMPLATE = 'GENERIC_TEMPLATE'
UNKNOWN_SEQ = 'UNKNOWN_SEQUENCE_OF_MESSAGE'
SEQ = 'SEQUENCE_OF_MESSAGE'
UNKNOWN = 'UNKNOWN'

THAI_LANG = 'TH'
EN_LANG = 'ENG'
UNKNOWN_LANG = 'UNKNOWN'

def main (channel_list, flow_detection_regex_lst, dict_of_per_user_intr_msg_lst, l_logger):
  global channel_lst, flow_regex_list, logger
  channel_lst = channel_list
  flow_regex_list = flow_detection_regex_lst
  logger = l_logger
  
  all_messages = []
  
  logger.info("----------------------------------------------------------")
  logger.info("User count: " + str(len(dict_of_per_user_intr_msg_lst)))
  for user_id in dict_of_per_user_intr_msg_lst:
    logger.info("User: " + str(user_id))
    msg_list = dict_of_per_user_intr_msg_lst[user_id]
    # Decode message from base 64 to utf-8
    decoded_msg_list = decode_msg_list(msg_list)
    # Sort by timestamp ASC
    sorted_msg_list = sorted(decoded_msg_list, key=lambda x: x['timestamp'])

    formatted_msg_list = []
    
    for msg_object in sorted_msg_list:
      
      formatted_msg_object = init_formatted_object(msg_object, user_id)
      result = {}
              
      fromUser = msg_object['source'] == 'user'
      if fromUser:
        result = classify_user_msg(msg_object, formatted_msg_object)
      else:
        result = classify_bot_msg(msg_object, formatted_msg_object)
      formatted_msg_list.append(result)  
    
    
    logger.info("User: " + str(user_id) + ", Message count: " + str(len(formatted_msg_list)))
    

      
    all_messages = all_messages + formatted_msg_list
  return all_messages

def decode_msg_list (msg_list):
  decoded_msg_list = []
  for msg_object in msg_list:
    decoded_msg = base64.b64decode(msg_object['message']).decode('utf-8')
    msg_object['message'] = decoded_msg
    decoded_msg_list.append(msg_object)
  return decoded_msg_list

def sort_msg_list_by_timestamp():
  pass

def init_formatted_object(msg_object, user_id):
  formatted_msg_object = {
  'channel_id': msg_object['channel_id'], 
  'id': str(uuid.uuid4()),
  'user_id': user_id,
  'thai_timestamp': datetime.fromtimestamp(msg_object['timestamp']/1000, thai_tz),
  'source': msg_object['source'],
  'interact_with': msg_object['interact_with'],
  'flow': UNKNOWN,
  'utc_timestamp': datetime.fromtimestamp(msg_object['timestamp']/1000, utc_tz)
  }
  return formatted_msg_object

def classify_user_msg(msg_object, formatted_msg_object):
  json_msg = json.loads(msg_object['message'])
  msg_type = ''
  msg = ''
  lang = ''
  
  # Get msg and msg type
  if 'quick_reply' in json_msg:
    msg_type= QUICK_REPLY
    msg = json_msg['text']
  elif 'attachments' in json_msg:
    msg_type = ATTACHMENT
    msg = json.dumps(json_msg['attachments'])
  else: 
    msg_type = TEXT
    msg = json_msg['text']
    
  formatted_msg_object['type'] = msg_type
  formatted_msg_object['message'] = msg
  # Get language
  formatted_msg_object['channel_type'] = next(ch['channel_type'] for ch in channel_lst if ch["channel_id"] == formatted_msg_object["channel_id"])
  if msg_type != ATTACHMENT:
    lang = THAI_LANG if re.search(thai_pattern, msg) else EN_LANG if re.search(eng_pattern, msg) else UNKNOWN_LANG
  else:
    lang = UNKNOWN_LANG
  formatted_msg_object['language'] = lang
  return formatted_msg_object

def classify_bot_msg(msg_object, formatted_msg_object):
  json_msg = json.loads(msg_object['message'])
  msg_type = ''
  lang = THAI_LANG if re.search(thai_pattern, msg_object["message"]) else EN_LANG if re.search(eng_pattern, msg_object["message"]) else UNKNOWN_LANG
  flow = ''
  msg = ''
  if len(json_msg) > 1:
    # Classify by regex in message[0]
    first_msg_obj = json_msg[0]['message']
    if 'text' in first_msg_obj:
      flow = find_match_regex(first_msg_obj['text'])
      # If it's in flow list
      if flow != None:
        msg = first_msg_obj['text']
        msg_type = SEQ
      # If not in flow list
      else:
        msg = first_msg_obj['text']
        msg_type = UNKNOWN_SEQ
  else:
    first_msg_obj = json_msg[0]['message']
    # Type text / quick reply
    if 'text' in first_msg_obj:
      flow = find_match_regex(first_msg_obj['text'])
      msg = first_msg_obj['text']
      
      if 'quickReplies' in first_msg_obj:
        if len(first_msg_obj['quickReplies']) > 0:
          msg_type = QUICK_REPLY
      else:
        msg_type = TEXT
        
      
    # Type Attachment
    elif 'attachment' in first_msg_obj:
      result = classify_attachment(first_msg_obj)
      msg_type = result['type']
      if msg_type == IMAGE:
        lang = UNKNOWN_LANG
      msg = result ['text']
      
  formatted_msg_object['type'] = msg_type
  formatted_msg_object['message'] = msg
  formatted_msg_object['language'] = lang
  formatted_msg_object['flow'] = flow if flow != '' and flow != None else UNKNOWN
  formatted_msg_object['channel_type'] = msg_object['channel_type']
  return formatted_msg_object

def classify_attachment(msg):
  attchmnt_msg = msg['attachment']
  payload = attchmnt_msg['payload']
  attchmnt_type = attchmnt_msg['type']
  result = {
    'type': '',
    'text': ''
  }
  text = ''
  type = ''
  if attchmnt_type == 'template':
    if payload['templateType'] == 'button':
      text = payload['text']
      type = BUTTON_TEMPLATE
    else:
      text = "<TEMPLATE>"
      type = GENERIC_TEMPLATE
  elif attchmnt_type == 'image':
    type = IMAGE
    text = "<IMAGE ATTACHMENT:" + payload['url'] + ">"
  else:
    type = UNKNOWN_ATTACH
    text = "<UNKNOWN ATTACHMENT>"
  
  result['text'] = text
  result['type'] = type
  return result

def find_match_regex(msg):
  for regex_item in flow_regex_list:
    for regex in regex_item['regexes']:
      s = re.search(r"{regex}".format(regex=regex), msg)
      if s:
        return regex_item['flow']
  return None