DROP TABLE IF EXISTS `Orders`;
DROP TABLE IF EXISTS `Customers`;

CREATE TABLE `Customers`
(
    `CustomerID` String,
    `CompanyName` String,
    `ContactName` String,
    `ContactTitle` String,
    `Address` String,
    `City` String,
    `Region` String,
    `PostalCode` String,
    `Country` String,
    `Phone` String,
    `Fax` String
)
ENGINE = MergeTree()
ORDER BY `CustomerID`;

CREATE TABLE `Orders`
(
    `OrderID` Int32,
    `CustomerID` String,
    `EmployeeID` Int32,
    `OrderDate` DateTime,
    `RequiredDate` DateTime,
    `ShippedDate` DateTime,
    `ShipVia` Int32,
    `Freight` Decimal(12, 2),
    `ShipName` String,
    `ShipAddress` String,
    `ShipCity` String,
    `ShipRegion` String,
    `ShipPostalCode` String,
    `ShipCountry` String
)
ENGINE = MergeTree()
ORDER BY (`CustomerID`, `OrderID`);

INSERT INTO `Customers`
(
    `CustomerID`,
    `CompanyName`,
    `ContactName`,
    `ContactTitle`,
    `Address`,
    `City`,
    `Region`,
    `PostalCode`,
    `Country`,
    `Phone`,
    `Fax`
)
VALUES
    ('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'Obere Str. 57', 'Berlin', '', '12209', 'Germany', '030-0074321', '030-0076545'),
    ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', '', 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750'),
    ('BSBEV', 'B''s Beverages', 'Victoria Ashworth', 'Sales Representative', 'Fauntleroy Circus', 'London', '', 'EC2 5NT', 'UK', '(171) 555-1212', ''),
    ('FOLKO', 'Folk och fä HB', 'Maria Larsson', 'Owner', 'Åkergatan 24', 'Bräcke', '', 'S-844 67', 'Sweden', '0695-34 67 21', ''),
    ('WOLZA', 'Wolski Zajazd', 'Zbyszek Piestrzeniewicz', 'Owner', 'ul. Filtrowa 68', 'Warsaw', '', '01-012', 'Poland', '(26) 642-7012', '(26) 642-7012');

INSERT INTO `Orders`
(
    `OrderID`,
    `CustomerID`,
    `EmployeeID`,
    `OrderDate`,
    `RequiredDate`,
    `ShippedDate`,
    `ShipVia`,
    `Freight`,
    `ShipName`,
    `ShipAddress`,
    `ShipCity`,
    `ShipRegion`,
    `ShipPostalCode`,
    `ShipCountry`
)
VALUES
    (10248, 'ALFKI', 5, '1996-07-04 00:00:00', '1996-08-01 00:00:00', '1996-07-16 00:00:00', 3, 32.38, 'Vins et alcools Chevalier', '59 rue de l''Abbaye', 'Reims', '', '51100', 'France'),
    (10249, 'WOLZA', 6, '1996-07-05 00:00:00', '1996-08-16 00:00:00', '1996-07-10 00:00:00', 1, 11.61, 'Toms Spezialitäten', 'Luisenstr. 48', 'Münster', '', '44087', 'Germany'),
    (10250, 'AROUT', 4, '1996-07-08 00:00:00', '1996-08-05 00:00:00', '1996-07-12 00:00:00', 2, 65.83, 'Hanari Carnes', 'Rua do Paço, 67', 'Rio de Janeiro', 'RJ', '05454-876', 'Brazil'),
    (10251, 'AROUT', 3, '1996-07-08 00:00:00', '1996-08-05 00:00:00', '1996-07-15 00:00:00', 1, 41.34, 'Victuailles en stock', '2, rue du Commerce', 'Lyon', '', '69004', 'France'),
    (10252, 'FOLKO', 4, '1996-07-09 00:00:00', '1996-08-06 00:00:00', '1996-07-11 00:00:00', 2, 51.30, 'Suprêmes délices', 'Boulevard Tirou, 255', 'Charleroi', '', 'B-6000', 'Belgium'),
    (10253, 'BSBEV', 3, '1996-07-10 00:00:00', '1996-07-24 00:00:00', '1996-07-16 00:00:00', 2, 58.17, 'Hanari Carnes', 'Rua do Paço, 67', 'Rio de Janeiro', 'RJ', '05454-876', 'Brazil');
