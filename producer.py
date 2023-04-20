from flask import Flask, request, render_template
from kafka import KafkaProducer
import pandas as pd
import io
from json import dumps

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

app = Flask(__name__,template_folder='templates')
  
UPLOAD_FOLDER = 'static/uploads/'
  
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        file = request.files.get("file")
        file_content = file.read()

        columns=['name','age']
        df = pd.read_csv(io.BytesIO(file_content), sep=",", header=None,names=columns)
        for index, row in df.iterrows():
            if(index==0):
                continue
            name = row['name']
            age = row['age']
            message = producer.send("sendRow", {"name":name, "age":age})
            data = message.get()
            print(data.topic, data.partition)
          
        if file_content:
            return "Uploaded Successful"
        else:
            return "Uploaded Unsuccessful"
  
    return render_template("index.html")
  
if __name__ == "__main__":
    app.run(debug=True)

# //////
## original source: https://www.geeksforgeeks.org/read-file-without-saving-in-flask/
##          source: https://medevel.com/flask-tutorial-upload-csv-file-and-insert-rows-into-the-database/
##          source: https://realpython.com/python-send-email/
# //////
## Inniciar APP
# python3 app.py    
## Acceder a: http://127.0.0.1:5000
## //////
# Paquetes a importar:
## pip install pandas
## pip install flask
# //////
