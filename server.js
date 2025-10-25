// server.js – Censo Partido Liberal 2025 (versión estable)
const express = require("express");
const cors = require("cors");
const duckdb = require("duckdb");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const https = require("https");
const fs = require("fs");

const app = express();
app.use(cors());
app.use(helmet());

// Limitar consultas por IP
app.use(rateLimit({ windowMs: 60 * 1000, max: 15 }));

// Ruta del archivo Parquet (en R2)
const PARQUET_URL = "https://pub-7ad254fd2edb413b968a33fff1a674d5.r2.dev/liberal.parquet";
// Archivo local temporal
const LOCAL_FILE = "/tmp/liberal.parquet";

// Conexión a DuckDB en memoria
const db = new duckdb.Database(":memory:");
const conn = db.connect();

// Ruta principal
app.get("/", (_req, res) => res.send("✅ API Censo Liberal Activa"));

// Descargar el parquet si no existe
function ensureParquet() {
  return new Promise((resolve, reject) => {
    try {
      if (fs.existsSync(LOCAL_FILE)) return resolve();
      console.log("Descargando liberal.parquet desde R2...");
      const file = fs.createWriteStream(LOCAL_FILE);
      https
        .get(PARQUET_URL, (res) => {
          if (res.statusCode !== 200)
            return reject(new Error(`Status ${res.statusCode} al descargar parquet`));
          res.pipe(file);
          file.on("finish", () => file.close(resolve));
        })
        .on("error", reject);
    } catch (e) {
      reject(e);
    }
  });
}

// Buscar por número de identidad
app.get("/buscar", async (req, res) => {
  try {
    const identidad = (req.query.identidad || "").replace(/[^\d]/g, "");
    if (!identidad || identidad.length < 6)
      return res.status(400).json({ error: "Número de identidad inválido" });

    await ensureParquet();

    const sql = `
      SELECT
        NUMERO_IDENTIDAD,
        PRIMER_NOMBRE, SEGUNDO_NOMBRE, PRIMER_APELLIDO, SEGUNDO_APELLIDO,
        SEXO, FECHA_NACIMIENTO, Edad,
        DEPARTAMENTO, MUNICIPIO, AREA, SECTOR,
        CODIGO_CENTRO, NOMBRE_CENTRO
      FROM read_parquet('${LOCAL_FILE}')
      WHERE NUMERO_IDENTIDAD = ?
      LIMIT 1;
    `;

    conn.all(sql, [identidad], (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      if (!rows || rows.length === 0)
        return res.json({ mensaje: "No encontrado" });
      res.json(rows[0]);
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API Liberal escuchando en http://localhost:${PORT}`);
});

