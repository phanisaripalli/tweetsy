from flask import Flask
from flask_restful import Api

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from api.resources.test import Test

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['JSON_AS_ASCII'] = False
api = Api(app)

from flask_cors import CORS
cors = CORS(app, resources={r"/*": {"origins": "*"}})


api.add_resource(Test,
                 '/test',
                 '/popular/',
                 endpoint='test',
                 strict_slashes=False
)


if __name__ == '__main__':
    app.run()
