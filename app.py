from flask import Flask, request, render_template, redirect
import pandas as pd
import cx_Oracle
import os

app = Flask(__name__)

db_path = os.path.join(app.root_path, 'data.db')

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temp_table (
            Código VARCHAR2(10 BYTE)
        )
    """)
    conn.commit()

def insert_data_into_temp_table(conn, df):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO temp_table (Código)
            VALUES (:1)
        """, (row['Código'],))
    conn.commit()

def fetch_cd_pro_fat(df_tuss, CD_TUSS):
    result = df_tuss.loc[df_tuss['CD_TUSS'] == CD_TUSS, 'CD_PRO_FAT'].values
    return result[0] if len(result) > 0 else ""

@app.route('/')
def index():
    return render_template("index.html")    

@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username == "admin" and password == "senha":
            return redirect("/upload")
        else:
            error_message = "Senha incorreta. Por favor, tente novamente."
            return render_template("index.html", error_message=error_message)
    else:
        return render_template("index.html")

@app.route('/<string:nome>')
def error(nome):
    variavel = f'ERROR 404: Página {nome} não existe'
    return render_template("error.html", variavel=variavel)

@app.route('/upload', methods=['POST', 'GET'])
def upload_xlsx():
    if request.method == 'POST':
        if 'file' not in request.files:
            return "Nenhum arquivo enviado."

        file = request.files['file']

        if file.filename == '':
            return "Nome de arquivo vazio."

        try:
            df = pd.read_excel(file)

            print("Colunas no DataFrame:")
            print(df.columns)  # Imprime as colunas do DataFrame

            print("Verificando a coluna 'Código':")
            if 'Código' in df.columns:
                print("A coluna 'Codigo' está presente no DataFrame.")
            else:
                print("A coluna 'Codigo' não está presente no DataFrame.")

            oracle_conn = cx_Oracle.connect("DBAMV/#Unimed#250@6021db.cloudmv.com.br:1522/tst16021.db6021.mv6021vcn.oraclevcn.com")
            create_table(oracle_conn)
            insert_data_into_temp_table(oracle_conn, df)
            oracle_conn.close()

            return redirect('/dados')
        except Exception as e:
            return f"Erro ao processar o arquivo: {str(e)}"
    else:
        return render_template("upload.html")


@app.route('/dados', methods=['POST', 'GET'])
def dados():
    oracle_conn = cx_Oracle.connect("DBAMV/#Unimed#250@6021db.cloudmv.com.br:1522/tst16021.db6021.mv6021vcn.oraclevcn.com")
    cursor_oracle = oracle_conn.cursor()
    try:
        cursor_oracle.execute("SELECT [Código] FROM temp_table")
        rows_temp = cursor_oracle.fetchall()
        codigos_temp = [row[0] for row in rows_temp]
        chunks = [codigos_temp[i:i+1000] for i in range(0, len(codigos_temp), 1000)]
        rows_tuss = []

        for chunk in chunks:
            placeholders = ', '.join(':' + str(i+1) for i in range(len(chunk)))
            query = """
                    SELECT t.CD_TUSS
                    FROM TUSS t
                    LEFT JOIN temp_table ON temp_table.[Código] = t.CD_TUSS
                    WHERE temp_table.[Código] IS NOT NULL AND t.CD_PRO_FAT IS NULL
                    """
            cursor_oracle.execute(query, chunk)
            rows_tuss.extend(cursor_oracle.fetchall())

        column_names_tuss = [description[0] for description in cursor_oracle.description]
        df_tuss = pd.DataFrame(rows_tuss, columns=column_names_tuss)

        column_names = column_names_tuss
        rows = df_tuss.values.tolist()

        return render_template("dados.html", column_names=column_names, rows=rows)

    finally:
        cursor_oracle.close()
        oracle_conn.close()


if __name__ == '__main__':
    app.run(debug=True)
