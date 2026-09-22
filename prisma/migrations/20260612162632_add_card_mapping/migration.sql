-- CreateTable
CREATE TABLE "card_mappings" (
    "id" TEXT NOT NULL,
    "card_uid" VARCHAR(20) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "linked_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "card_mappings_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "card_mappings_card_uid_key" ON "card_mappings"("card_uid");

-- CreateIndex
CREATE UNIQUE INDEX "card_mappings_student_uid_key" ON "card_mappings"("student_uid");
