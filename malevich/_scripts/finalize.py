import argparse
from datetime import datetime
import os
import sys
import dill

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

parser = argparse.ArgumentParser()
parser.add_argument('session_id', type=int)
args = parser.parse_args()

os.environ['_MALEVICH_NO_FINALIZE'] = 'true'

from malevich._db import Read, Write
from malevich._db.schema.artifacts import Artifact, MasterSession, Session
from malevich.path import Paths

with Write() as db_ref:
    session = db_ref.get(Session, args.session_id)
    # session_artifacts = db_ref.query(Artifact).filter(
    #     Artifact.session_id == args.session_id
    # ).all()

    # declared_flows = set()
    # files = set()

    # for artifact in session_artifacts:
    #     artifact.payload = dill.loads(artifact.payload)
    #     if artifact.payload['type'] == 'flow_declaration':
    #         declared_flows.add(artifact.payload['reverse_id'])
    #     if artifact.payload['type'] == 'file_content':
    #         files.add(artifact.payload['file'])

    # recent_artifacts = db_ref.query(Artifact).all()
    session.end = datetime.now()
    db_ref.commit()
