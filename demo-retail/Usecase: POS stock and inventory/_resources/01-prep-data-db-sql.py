# Databricks notebook source
# MAGIC %md
# MAGIC # Setup database for DBSQL. Run only once.

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists field_demos_retail;
# MAGIC create table if not exists field_demos_retail.pos_store LOCATION '/mnt/field-demos/retail/pos/store';
# MAGIC create table if not exists field_demos_retail.pos_item LOCATION '/mnt/field-demos/retail/pos/item';
# MAGIC create table if not exists field_demos_retail.pos_inventory_change LOCATION '/mnt/field-demos/retail/pos/inventory_change';
# MAGIC create table if not exists field_demos_retail.pos_inventory_current LOCATION '/mnt/field-demos/retail/pos/inventory_current';
# MAGIC create table if not exists field_demos_retail.pos_history_inventory LOCATION '/mnt/field-demos/retail/pos/history_inventory';
