from flask import Flask, Response
from prometheus_client import Summary, generate_latest, CONTENT_TYPE_LATEST

s = Summary('analyse_seconds', 'Number of seconds to find matches', ['modvar'])

app = Flask(__name__)

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route('/metric/<model>/<variable>/<int:duration>', methods=['PUT'])
def metric(model, variable, duration):
    s.labels(modvar=model + '.' + variable).observe(duration)
    return Response()

if __name__ == '__main__':
    app.run()
