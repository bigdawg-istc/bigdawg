CREATE TABLE dimaccount00 AS (
    SELECT * FROM dimaccount
    WHERE accountid < 1511);

CREATE TABLE dimaccount01 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511 and accountid < 1511*2);

CREATE TABLE dimaccount02 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*2 and accountid < 1511*3);

CREATE TABLE dimaccount03 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*3 and accountid < 1511*4);

CREATE TABLE dimaccount04 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*4 and accountid < 1511*5);

CREATE TABLE dimaccount05 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*5 and accountid < 1511*6);

CREATE TABLE dimaccount06 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*6 and accountid < 1511*7);

CREATE TABLE dimaccount07 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*7 and accountid < 1511*8);

CREATE TABLE dimaccount08 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*8 and accountid < 1511*9);

CREATE TABLE dimaccount09 AS (
    SELECT * FROM dimaccount
    WHERE accountid >= 1511*9 and accountid < 1511*10);

