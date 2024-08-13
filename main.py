import requests
import argparse
from time import sleep
from typing import Optional, Any, List
from dataclasses import dataclass, field
import re
import sys

@dataclass
class QdbEntity:
    eid: str
    etype: str
    name: str
    fields: dict[str, Any] = field(default_factory=dict)

class QdbClient:
    def __init__(self, url: str) -> None:
        self._url: str = url.rstrip('/')

    def __extract_type_and_value(self, s: str) -> tuple[Optional[str], Optional[Any]]:
        pattern = r'(?P<type>qdb\.\w+)\((?P<value>.+)\)'
        
        typeMap = {
            "qdb.Int": int,
            "qdb.Float": float,
            "qdb.String": str,
            "qdb.EntityReference": str,
            "qdb.Bool": bool,
            "qdb.Timestamp": str,
            "qdb.ConnectionState": str,
            "qdb.GarageDoorState": str,
        }

        match = re.search(pattern, s)
        
        if match and match.group('type') in typeMap:
            caster = typeMap[match.group('type')]
            return match.group('type'), caster(match.group('value'))
        else:
            return None, None

    def message_template(self) -> dict[str, Any]:
        return requests.get(f"{self._url}/make-client-id").json()

    def get_entity(self, entityId: str, template: Optional[dict[str, Any]]=None) -> QdbEntity:
        if template is None:
            template = self.message_template()
        
        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeGetEntityRequest",
                "entityId": entityId
            }
        })

        response = requests.post(f"{self._url}/api", json=request).json()
        entity = response['payload']['entity']
        return QdbEntity(entity["id"], entity["type"], entity["name"])

    def get_entities(self, entityType: str, template: Optional[dict[str, Any]]=None) -> List[QdbEntity]:
        if template is None:
            template = self.message_template()
        
        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeGetEntitiesRequest",
                "entityType": entityType
            }
        })

        response = requests.post(f"{self._url}/api", json=request).json()
        return [QdbEntity(e["id"], e["type"], e["name"]) for e in response['payload']['entities']]

    def read(self, entityTypeOrId: str, fields: List[str], template: Optional[dict[str, Any]]=None) -> List[QdbEntity]:
        if template is None:
            template = self.message_template()

        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeDatabaseRequest",
                "requestType": "READ",
                "requests": []
            }
        })

        entities = []

        if '-' in entityTypeOrId:
            entities.append(self.get_entity(entityTypeOrId, template))
        else:
            entities = self.get_entities(entityTypeOrId, template)
        
        for entity in entities:
            for field in fields:
                request["payload"]["requests"].append({
                    "entityId": entity.eid,
                    "field": field
                })
        
        response = requests.post(f"{self._url}/api", json=request).json()

        entityById = {entity.eid: entity for entity in entities}
        for entity in response['payload']['response']:
            entityById[entity["id"]].fields[entity["field"]] = entity["value"].get("raw")

        return entities

    def write(self, entityId: str, fields: dict[str, Any], template: Optional[dict[str, Any]]=None) -> bool:
        if template is None:
            template = self.message_template()

        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeDatabaseRequest",
                "requestType": "WRITE",
                "requests": []
            }
        })
        
        for field, value in fields.items():
            typeName, value = self.__extract_type_and_value(value)
            if typeName is None:
                print(f"Failed to write field '{field}' with value '{value}'. Invalid value type.")
                return False
            request["payload"]["requests"].append({
                "entityId": entityId,
                "field": field,
                "value": {
                    "@type": "type.googleapis.com/" + typeName,
                    "raw": value
                }
            })

        response = requests.post(f"{self._url}/api", json=request).json()
        return all(r["success"] for r in response['payload']['response'])

    def register_notification(self, entityTypeOrId: str, field: str, context: List[str], notifyOnChange: bool, template: Optional[dict[str, Any]]=None) -> bool:
        if template is None:
            template = self.message_template()

        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeRegisterNotificationRequest",
                "requests": [
                    {
                        "field": field,
                        "contextFields": context,
                        "notifyOnChange": notifyOnChange,
                    }
                ]
            }
        })
        
        if '-' in entityTypeOrId:
            request["payload"]["requests"][0]["id"] = entityTypeOrId
        else:
            request["payload"]["requests"][0]["type"] = entityTypeOrId

        response = requests.post(f"{self._url}/api", json=request).json()
        return len(response["payload"]["tokens"]) > 0

    def get_notifications(self, template: Optional[dict[str, Any]]=None) -> List[dict[str, Any]]:
        if template is None:
            template = self.message_template()

        request = {}
        request.update(template)
        request.update({
            "payload": {
                "@type": "type.googleapis.com/qdb.WebRuntimeGetNotificationsRequest"
            }
        })

        response = requests.post(f"{self._url}/api", json=request).json()
        return response["payload"]["notifications"]

    def listen(self, entityTypeOrId: str, field: str, context: List[str], notifyOnChange: bool) -> None:
        if self.register_notification(entityTypeOrId, field, context, notifyOnChange):
            try:
                while True:
                    notifications = self.get_notifications()
                    for notification in notifications:
                        print(notification)
                        print()
                    sleep(1)
            except KeyboardInterrupt:
                pass

def main():
    parser = argparse.ArgumentParser(description="CLI tool for interacting with the QDB API.")
    
    parser.add_argument("--url", type=str, default="http://database.local", help="The base URL for the QDB API.")
    
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Read Command
    read_parser = subparsers.add_parser("read", help="Read fields from an entity or entity type.")
    read_parser.add_argument("entity", type=str, help="The entity type or entity ID.")
    read_parser.add_argument("fields", nargs='+', type=str, help="The fields to read.")

    # Write Command
    write_parser = subparsers.add_parser("write", help="Write fields to an entity.")
    write_parser.add_argument("entityId", type=str, help="The entity ID.")
    write_parser.add_argument("fields", nargs='+', type=str, help="The fields to write in 'field=value' format.")

    # Listen Command
    listen_parser = subparsers.add_parser("listen", help="Listen for notifications on an entity or entity type.")
    listen_parser.add_argument("entity", type=str, help="The entity type or entity ID.")
    listen_parser.add_argument("field", type=str, help="The field to listen to.")
    listen_parser.add_argument("--context", nargs='*', type=str, default=[], help="The context fields for the notification.")
    listen_parser.add_argument("--notifyOnChange", action="store_true", help="Notify on field changes.")

    args = parser.parse_args()

    client = QdbClient(args.url)

    if args.command == "read":
        entities = client.read(args.entityTypeOrId, args.fields)
        for entity in entities:
            print(f"Entity ID: {entity.eid}, Type: {entity.etype}, Name: {entity.name}")
            for field, value in entity.fields.items():
                print(f"  {field}: {value}")
            print()

    elif args.command == "write":
        fields = {k: v for k, v in (field.split('=') for field in args.fields)}
        success = client.write(args.entityId, fields)
        print("Write successful" if success else "Write failed")

    elif args.command == "listen":
        client.listen(args.entityTypeOrId, args.field, args.context, args.notifyOnChange)

if __name__ == '__main__':
    main()
