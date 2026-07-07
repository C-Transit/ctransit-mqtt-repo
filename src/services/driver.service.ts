"use strict";

import { prisma } from "./ledger.service.js";
import bcrypt from "bcryptjs";
import logger from "../config/logger.js";

export interface DriverRegisterData {
  firstname: string;
  lastname: string;
  matricNumber: string;
}

async function handleDriverRegister(
  terminalId: string,
  data: DriverRegisterData
) {
  const { firstname, lastname, matricNumber } = data;
  const log = logger.child({ terminalId, matricNumber });

  // Check if driver already exists
  const existing = await prisma.user.findUnique({
    where: { matricNumber: matricNumber.toUpperCase() },
  });

  if (existing) {
    log.warn("driver.already_registered");
    return {
      success: false,
      message: `${matricNumber} already registered`,
    };
  }

  const defaultPassword = `${matricNumber.replace(/\//g, "")}Driver!`;
  const hashedPassword = await bcrypt.hash(defaultPassword, 10);

  await prisma.user.create({
    data: {
      firstname,
      lastname,
      email: `-`,
      matricNumber: matricNumber.toUpperCase(),
      password: hashedPassword,
      role: "DRIVER",
      isVerified: true,
    },
  });

  log.info({ firstname, lastname }, "driver.registered_from_terminal");

  return {
    success: true,
    message: `Driver ${firstname} ${lastname} registered`,
  };
}

async function handleDriverLogin(terminalId: string, driverUid: string) {
  const log = logger.child({ terminalId, driverUid });

  const driver = await prisma.user.findFirst({
    where: {
      matricNumber: driverUid.toUpperCase(),
      role: "DRIVER",
    },
    select: { id: true, firstname: true, lastname: true, matricNumber: true },
  });

  if (!driver) {
    log.warn("driver.not_found_or_not_driver_role");
    return { success: false, message: "Driver not recognised." };
  }

  await prisma.terminal.update({
    where: { terminal_id: terminalId },
    data: { active_driver_uid: driver.matricNumber },
  });

  log.info("driver.logged_into_terminal");

  return {
    success: true,
    message: `Welcome ${driver.firstname}`,
    driverName: `${driver.firstname} ${driver.lastname}`,
  };
}

async function handleDriverLogout(terminalId: string, driverUid: string) {
  const log = logger.child({ terminalId, driverUid });

  await prisma.terminal.update({
    where: { terminal_id: terminalId },
    data: { active_driver_uid: null },
  });

  log.info("driver.logged_out_of_terminal");
  return { success: true, message: "Driver logged out." };
}

export { handleDriverRegister, handleDriverLogin, handleDriverLogout };

// ─────────────────────────────────────────────
// listDrivers
// Returns all drivers with their active terminal
// assignment. Uses a Map for the join so we avoid
// an N+1 query — one drivers query, one terminals
// query, merged in memory.
// ─────────────────────────────────────────────
async function listDrivers() {
  const [drivers, activeTerminals] = await Promise.all([
    prisma.user.findMany({
      where: { role: "DRIVER" },
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        firstname: true,
        lastname: true,
        matricNumber: true,
        createdAt: true,
      },
    }),
    // Only fetch terminals that currently have a driver — avoids
    // scanning the full terminal table when most are empty
    prisma.terminal.findMany({
      where: { active_driver_uid: { not: null } },
      select: {
        terminal_id: true,
        status: true,
        active_driver_uid: true,
      },
    }),
  ]);

  // Map: matricNumber → terminal — O(1) lookups in the merge below
  const terminalByDriver = new Map(
    activeTerminals.map((t) => [t.active_driver_uid, t])
  );

  return drivers.map((driver) => ({
    ...driver,
    activeTerminal: terminalByDriver.get(driver.matricNumber) ?? null,
  }));
}

// ─────────────────────────────────────────────
// registerDriverByAgent
// REST-based driver registration by an agent.
// Unlike handleDriverRegister (MQTT flow which
// requires a terminalId), this is initiated from
// the agent dashboard with no terminal context.
// Same default password scheme as the MQTT path
// for consistency — agent communicates it to the
// driver out-of-band.
// ─────────────────────────────────────────────
async function registerDriverByAgent(data: DriverRegisterData) {
  const { firstname, lastname, matricNumber } = data;
  const normalisedMatric = matricNumber.toUpperCase();

  const existing = await prisma.user.findUnique({
    where: { matricNumber: normalisedMatric },
    select: { id: true, role: true },
  });

  if (existing) {
    // Surface a clear error code — controller maps it to 409
    throw new Error(
      existing.role === "DRIVER"
        ? "DRIVER_ALREADY_EXISTS"
        : "MATRIC_NUMBER_IN_USE"
    );
  }

  const defaultPassword = `${normalisedMatric.replace(/\//g, "")}Driver!`;
  const hashedPassword = await bcrypt.hash(defaultPassword, 10);

  const driver = await prisma.user.create({
    data: {
      firstname: firstname.trim(),
      lastname: lastname.trim(),
      email: `-`,
      matricNumber: normalisedMatric,
      password: hashedPassword,
      role: "DRIVER",
      isVerified: true,
    },
    select: {
      id: true,
      firstname: true,
      lastname: true,
      matricNumber: true,
      createdAt: true,
    },
  });

  logger.info(
    { matricNumber: driver.matricNumber },
    "driver.registered_by_agent"
  );

  return driver;
}

export { listDrivers, registerDriverByAgent };
