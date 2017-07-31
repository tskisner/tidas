/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

    // forward declare copy functions

    class block;

    void data_copy ( intervals const & in, intervals & out );
    void data_copy ( group const & in, group & out );
    void data_copy ( block const & in, block & out );


    /// A block represents a logical grouping of data.  A block can have zero
    /// or more groups, intervals and sub-blocks.  Child objects are added
    /// with a "name" associated to them.  These names are used when performing
    /// selection / query operations.  
    /// Some public methods are only used internally and are not needed for
    /// normal use of the object.  These are labelled "internal".

    class block {

        public :

            /// Default constructor.
            block ();

            /// Destructor
            ~block ();
            
            /// Assignment operator.
            block & operator= ( block const & other );

            /// Copy constructor.
            block ( block const & other );

            /// (**Internal**) Create a block from disk with the specified 
            /// selection filter.
            block ( backend_path const & loc, std::string const & filter = "" );
            
            /// (**Internal**) Create a copy of a block, with optional selection and new location.
            block ( block const & other, std::string const & filter, backend_path const & loc );

            // metadata ops

            /// (Internal) Change the location of the block.
            void relocate ( backend_path const & loc );

            /// (Internal) Recursively read the metadata for the block and its
            /// children from disk.  Optionally apply a selection filter.
            void sync ( std::string const & filter = "" );

            /// (Internal) Recursively write metadata for the block and its 
            /// children.
            void flush () const;

            /// (Internal) Copy
            void copy ( block const & other, std::string const & filter, backend_path const & loc );

            // Create a link at the specified location.
            void link ( link_type const & type, std::string const & path ) const;

            // Delete the on-disk data and metadata associated with this object.
            // In-memory metadata is not modified.
            void wipe () const;

            backend_path location () const;


            /// Return the filesystem path of the auxilliary directory for 
            /// this block.
            std::string aux_dir () const;

            // data ops

            void range ( time_type & start, time_type & stop ) const;

            void clear();


            group & group_add ( std::string const & name, group const & grp );

            group & group_get ( std::string const & name );

            group const & group_get ( std::string const & name ) const;
            
            void group_del ( std::string const & name );

            std::vector < std::string > all_groups () const;
            
            void clear_groups();


            intervals & intervals_add ( std::string const & name, intervals const & intr );
            
            intervals & intervals_get ( std::string const & name );

            intervals const & intervals_get ( std::string const & name ) const;
            
            void intervals_del ( std::string const & name );
            
            std::vector < std::string > all_intervals () const;
            
            void clear_intervals();


            block & block_add ( std::string const & name, block const & blk );
            
            block & block_get ( std::string const & name );

            block const & block_get ( std::string const & name ) const;
            
            void block_del ( std::string const & name );
            
            std::vector < std::string > all_blocks () const;
            
            void clear_blocks();


            block select ( std::string const & filter = "" ) const;

            void info ( std::string name, bool recurse, size_t indent );


            // non-const version

            template < class P >
            void exec ( P & op, exec_order order ) {

                if ( order == exec_order::depth_last ) {
                    op ( *this );
                }

                if ( block_data_.size() == 0 ) {

                    // this is a leaf
                    if ( order == exec_order::leaf ) {
                        op ( *this );
                    }
                
                } else {
                    // operate on sub blocks

                    for ( std::map < std::string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
                        it->second.exec ( op, order );
                    }

                }

                if ( order == exec_order::depth_first ) {
                    op ( *this );
                }

                return;
            }


            // const version
            
            template < class P >
            void exec ( P & op, exec_order order ) const {

                if ( order == exec_order::depth_last ) {
                    op ( *this );
                }

                if ( block_data_.size() == 0 ) {

                    // this is a leaf
                    if ( order == exec_order::leaf ) {
                        op ( *this );
                    }
                
                } else {
                    // operate on sub blocks

                    for ( std::map < std::string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
                        it->second.exec ( op, order );
                    }

                }

                if ( order == exec_order::depth_first ) {
                    op ( *this );
                }

                return;
            }

            template < class Archive >
            void save ( Archive & ar ) const {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( intervals_data_ ) );
                ar ( CEREAL_NVP( group_data_ ) );
                ar ( CEREAL_NVP( block_data_ ) );
            }

            template < class Archive >
            void load ( Archive & ar ) {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( intervals_data_ ) );
                ar ( CEREAL_NVP( group_data_ ) );
                ar ( CEREAL_NVP( block_data_ ) );
            }

        private :

            backend_path group_loc ( backend_path const & loc, std::string const & name ) const;
            
            backend_path intervals_loc ( backend_path const & loc, std::string const & name ) const;
            
            backend_path block_loc ( backend_path const & loc, std::string const & name ) const;

            std::map < std::string, block > block_data_;
            std::map < std::string, group > group_data_;
            std::map < std::string, intervals > intervals_data_;

            backend_path loc_;

    };


    class block_link_operator {

        public :

            void operator() ( block const & blk );

            link_type type;
            std::string path;
            std::string start;

    };


    class block_wipe_operator {

        public :

            void operator() ( block const & blk );

    };


}

#endif
