import requests
from time import sleep
from typing import Optional, Any
from dataclasses import dataclass

@dataclass
class QdbEntity:
    eid: str
    etype: str
    name: str
    fields: dict[str, Any] = {}

class QdbClientInterface:
    def __init__(self, url: str) -> None:
        self._url: str = url

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

    def get_entities(self, entityType, template: Optional[dict[str, Any]]=None) -> list[QdbEntity]:
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

    def read(self, entityTypeOrId: str, fields: list[str], template: Optional[dict[str, Any]]=None) -> list[QdbEntity]:
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

        typeMap = {
            int: "type.googleapis.com/qdb.Int",
            float: "type.googleapis.com/qdb.Float",
            str: "type.googleapis.com/qdb.String",
            bool: "type.googleapis.com/qdb.Bool"
        }
         
        for field, value in fields.items():
            request["payload"]["requests"].append({
                "entityId": entityId,
                "field": field,
                "value": {
                    "@type": typeMap[type(value)],
                    "raw": value
                }
            })

        response = requests.post(f"{self._url}/api", json=request).json()
        return all(r["success"] for r in response['payload']['response'])

    def register_notification(self, entityTypeOrId: str, field: str, context: list[str]) -> None:
        pass

    def get_notifications(self) -> list[dict[str, Any]]:
        pass

    def listen(self, entityTypeOrId: str, field: str, context: list[str]) -> None:
        try:
            while True:
                pass
                sleep(1)
        except KeyboardInterrupt:
            pass

class QdbApplication:
    def __init__(self) -> None:
        pass

    def exec(self) -> None:
        pass

if __name__ == '__main__':
    app = QdbApplication()
    app.exec()