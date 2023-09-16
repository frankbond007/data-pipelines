from datetime import datetime, timezone

def convert_to_utc(timestamp):
    if timestamp is None:
        return None

    if isinstance(timestamp, int):
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    elif isinstance(timestamp, str):
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    else:
        raise ValueError("Unsupported timestamp format")
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat()


def unify_structure(input_json):

    key_mapping = {
        'type': 'type',
        'id': 'id',
        'state': 'status',
        'status': 'status',
        'title': 'title',
        'user': 'user',
        'body': 'body',
        'labels': 'tags',
        'tags': 'tags',
        'closed_at': 'closed_or_merged_at',
        'merged_at': 'closed_or_merged_at',
        'started_at': 'created_or_started_at',
        'created_at': 'created_or_started_at',
        'repository': 'repository'
    }

    unified = {}

    for input_key, unified_key in key_mapping.items():
        try:
            if input_key in input_json:
                if input_key in ['closed_at', 'started_at', 'merged_at', 'created_at']:
                    unified[unified_key] = convert_to_utc(input_json[input_key])
                elif input_key == 'repository' and isinstance(input_json[input_key], str):
                    repo_name = input_json['repository'].split('/')[-1]
                    repo_owner = input_json['repository'].split('/')[-2]
                    unified['repository'] = {
                        'name': repo_name,
                        'owner': repo_owner
                    }
                else:
                    unified[unified_key] = input_json[input_key]
        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"""Data is not in expected format, one of the fields defined
            in the requirements is either missing or is in the wrong format. The Data will be rejected and written
            to events_rejected data table.""")
            return input_json
    return unified
