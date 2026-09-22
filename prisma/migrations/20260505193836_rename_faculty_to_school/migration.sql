/*
  Warnings:

  - You are about to drop the column `faculty` on the `kyc_records` table. All the data in the column will be lost.
  - Added the required column `school` to the `kyc_records` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "kyc_records" DROP COLUMN "faculty",
ADD COLUMN     "school" TEXT NOT NULL;
