/*
  Warnings:

  - Added the required column `phoneNumber` to the `kyc_records` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "kyc_records" ADD COLUMN     "phoneNumber" TEXT NOT NULL;
