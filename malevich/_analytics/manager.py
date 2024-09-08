import atexit
import base64
import logging
import os
from datetime import datetime
from typing import Any

import dill

from malevich._db import Write

from .._db.schema.artifacts import Session
from .utils import decide_session

logger = logging.getLogger('malevich.artifacts')

class ArtifactsManager:
    session_id = decide_session()

    @staticmethod
    def write_artifact(payload) -> None:
        from .._db.schema.artifacts import Artifact

        with Write() as db_ref:

            artifact = Artifact(
                payload=base64.b64encode(dill.dumps(payload)).decode(),
                captured_at=datetime.now(),
                session_id=ArtifactsManager.session_id
            )
            db_ref.add(artifact)
            db_ref.commit()
            pld_str = str(payload)
            if len(pld_str) > 40:
                pld_str = pld_str[:20] + '...' + pld_str[-17:]

            logger.debug(
                f'New artifact: id={artifact.id}, session={ArtifactsManager.session_id}, content={pld_str}'
            )

    @staticmethod
    def finalize_session() -> None:
        import subprocess

        import malevich

        script_path = os.path.join(os.path.dirname(malevich.__file__), '_scripts', 'finalize.py')
        subprocess.run(['python', script_path, str(ArtifactsManager.session_id)])

    @staticmethod
    def get_artifact(id: int) -> dict[str, Any]:
        from .._db.schema.artifacts import Artifact

        with Write() as db_ref:
            artifact = db_ref.query(Artifact).get(id)
            return dill.loads(base64.b64decode(artifact.payload))

    @staticmethod
    def get_all_artifacts() -> list[dict[str, Any]]:
        from .._db.schema.artifacts import Artifact

        with Write(timeout=2) as db_ref:
            artifacts = db_ref.query(Artifact).all()
            return [
                dill.loads(base64.b64decode(artifact.payload))
                for artifact in artifacts
            ]

    @staticmethod
    def get_session_artifacts(session_id: int) -> list[dict[str, Any]]:
        from .._db.schema.artifacts import Artifact

        with Write() as db_ref:
            artifacts = db_ref.query(Artifact).filter(
                Artifact.session_id == session_id
            ).all()
            return [
                dill.loads(base64.b64decode(artifact.payload))
                for artifact in artifacts
            ]

    @staticmethod
    def get_grouped_artifacts() -> dict[int, dict[str, Any]]:
        from .._db.schema.artifacts import Artifact

        with Write() as db_ref:
            artifacts = db_ref.query(Artifact).all()
            grouped = {}
            for artifact in artifacts:
                if artifact.session_id not in grouped:
                    grouped[artifact.session_id] = []
                grouped[artifact.session_id].append(artifact)
            return grouped



if not os.getenv('_MALEVICH_NO_FINALIZE'):
    atexit.register(ArtifactsManager.finalize_session)
