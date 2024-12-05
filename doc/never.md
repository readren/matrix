Qué no hacer

- Inicializar una variable miembro usando algún método no privado a la clase `A` que lo contiene.
  Razón: Porque el hecho de no ser privado ofrece a toda subclase `B <: A` la posibilidad de pisarlo; lo cual es peligroso porque el método es llamado durante la construcción de `A`, antes que la subclase `B` sea inicializada, y se lanzaría NullPointerException si accede a cualquier variable miembro adquirida por `B`. 
- 
- 
- 

