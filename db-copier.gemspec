# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = 'db-copier'
  s.version = "0.0.1"

  s.authors = ["Santosh Kumar"]
  s.date = %q{2010-01-18}
  s.email = %q{santosh79@gmail.com}
  s.files = [
    "lib/db-copier.rb",
    "lib/db-copier/db-copier.rb",
    "lib/db-copier/worker.rb",
    "README.textile",
    "spec/spec_helper.rb",
    "spec/basic_spec.rb"]
  s.homepage = %q{http://github.com/santosh79/db-copier}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.5}
  s.summary = %q{A DSL around Sequel to aid in copying or replicating databases.}
  #s.add_dependency('sequel', '>=3.8.0')
  #s.add_dependency('term-ansicolor','1.0.4')

end
