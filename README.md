# rekt

Redis Key Tree

Prints information about data in Redis. Note that this assumes your keys are organized in a hierarchy, using ":" as a delimter.

## Prerequisites

<pre>
go get github.com/cupcake/rdb
</pre>

## Installation

<pre>
go get github.com/dbertouille/rekt
</pre>

## Usage

Retrieve `dumb.rdb` from your redis instance. This is usually located at `/var/lib/redis/dumb.rdb`.

<pre>
cat dump.rdb | rekt
</pre>