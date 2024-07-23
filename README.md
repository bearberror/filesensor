Start with making missing directory

`mkdir ./logs,./plugins,./config,./source,./target`

Start Airflow service

`docker compose up -d`

See Airflow UI at

`http://localhost:8080`

username: airflow
pw: airflow


อย่าลืมสร้าง fs_conn_id ในหน้า Airflow UI

ให้เข้าไปที่ Admin >> Connections >> กดบวก add connection 

เลือก Connection type เป็น File(path)

เติมต่า Connection ID, Path = directory ที่จะให้ Sensor มอง

การเซทค่า Timeout และ poke_interval ของ Sensor

`tiemout` คือ ระยะเวลาสูงสุดที่ Sensor จะคอยทำงานจนกว่าจะขึ้น Status: Skipped
`poke_interval` คือระยะเวลาระหว่าง poke 1 >> poke 2 

แสดงว่าจำนวนครั้งที่ Sensor จะทำงาน = timeout/poke_interval
