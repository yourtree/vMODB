# Microservice-Oriented Database System (MODB)

MODB is a distributed, event-driven, microservice-oriented database management system. The first principled approach for designing stateful microservices that require advanced data management requirements.

MODB offers event-driven functionalities by design, which makes it easy for developers to build stateful microservices that run on the cloud, at the same time offering abstractions developers are more used to work with.

MODB runs application logic in seamlessly within the application tier, not requiring developers to implement complex and error-prone stored procedures in the database.

Developers can plug and play any microservice at any time, the MODB then adapt the event streams seamlessly as requested by the application. In the end, developers still experience the flexibility and dinamicity offered by microservice architectures, but with built-in system-level database support to alleviate several challenges usually found in the practice.

## Table of Contents
- [Getting Started](#getting-started)
    * [Prerequisites](#prerequisites)
    * [Abstractions](#abstractions)
    * [Architecture](#architecture)
- [MODB](#modb)
    * [Configuration](#config)
    * [APIs](#apis)
    * [Play Around](#play)
    * [Testing](#test)

## <a name="getting-started"></a>Getting Started

### <a name="prerequisites"></a>Prerequisites

- JDK 18 (if you want to modify the source code)
- Curl (if you want to play with the APIs)

## <a name="modb"></a>MODB

Differently from traditional server-based database systems, where users interact via a well-defined network protocol, in MODB, users solely write code and all the data management complexity is abstracted by the runtime.

