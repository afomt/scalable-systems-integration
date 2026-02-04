from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/analytics/data", methods=["POST"])
def receive_data():
    print("Received analytics payload:", request.json)
    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)
