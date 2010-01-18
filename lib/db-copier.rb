libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require 'rubygems'
require 'sequel'
require 'term/ansicolor'
include Term::ANSIColor
require 'db-copier/db-copier'
require 'db-copier/worker'
