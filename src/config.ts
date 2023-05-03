import dotenv from "dotenv";
import express from "express";
import firebaseAdmin from "firebase-admin";
import { initializeApp } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";
import { Kafka } from "kafkajs";

// Firebase config
dotenv.config();
const firebaseKey = JSON.parse(process.env.FIREBASE_KEY ?? "{}");
const firebaseApp = initializeApp({
  credential: firebaseAdmin.credential.cert(firebaseKey),
});
export const auth = getAuth(firebaseApp);
export const firestore = getFirestore(firebaseApp);

// Kafka config
export const kafka = new Kafka({
  clientId: "igni",
  brokers: ["localhost:9092"],
});

// Express config
export const app = express();
